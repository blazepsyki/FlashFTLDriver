#include "lsmtree.h"
#include "io.h"
#include <stdlib.h>
#include <getopt.h>
#include <math.h>
#include "function_test.h"
#include "segment_level_manager.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

lsmtree LSM;
char all_set_page[PAGESIZE];
void *lsmtree_read_end_req(algo_req *req);
static void processing_data_read_req(algo_req *req, char *v, bool);
typedef std::map<uint32_t, algo_req*>::iterator rb_r_iter;
page_read_buffer rb;
extern uint32_t debug_lba;
int tiered_level_fd;

struct algorithm lsm_ftl={
	.argument_set=lsmtree_argument_set,
	.create=lsmtree_create,
	.destroy=lsmtree_destroy,
	.read=lsmtree_read,
	.write=lsmtree_write,
	.flush=lsmtree_flush,
	.remove=lsmtree_remove,
	.test=lsmtree_testing,
};

static void tiered_level_fd_open(){
	tiered_level_fd=open("last_level_dump", O_CREAT | O_TRUNC | O_RDWR, 0666);
	if(tiered_level_fd==-1){
		EPRINT("file open error", true);
	}
}

static inline void lsmtree_monitor_init(){
	measure_init(&LSM.monitor.RH_check_stopwatch[0]);
	measure_init(&LSM.monitor.RH_check_stopwatch[1]);

	measure_init(&LSM.monitor.RH_make_stopwatch[0]);
	measure_init(&LSM.monitor.RH_make_stopwatch[1]);
}

uint32_t lsmtree_create(lower_info *li, blockmanager *bm, algorithm *){
//	tiered_level_fd_open();
	io_manager_init(li);
	LSM.pm=page_manager_init(bm);
	LSM.cm=compaction_init(COMPACTION_REQ_MAX_NUM);
	LSM.wb_array=(write_buffer**)malloc(sizeof(write_buffer*) * WRITEBUFFER_NUM);
	LSM.now_wb=0;
	for(uint32_t i=0; i<WRITEBUFFER_NUM; i++){
	//	LSM.wb_array[i]=write_buffer_init(KP_IN_PAGE-L2PGAP, LSM.pm, NORMAL_WB);
		LSM.wb_array[i]=write_buffer_init(QDEPTH, LSM.pm, NORMAL_WB);
	}

	LSM.disk=(level**)calloc(LSM.param.LEVELN, sizeof(level*));
	LSM.level_rwlock=(rwlock*)malloc(LSM.param.LEVELN * sizeof(rwlock));

	uint32_t now_level_size=LSM.param.normal_size_factor;
	for(uint32_t i=0; i<LSM.param.LEVELN; i++){
		if(i<=LSM.param.tr.border_of_leveling){
			if(i<=LSM.param.tr.border_of_wisckey){
				LSM.disk[i]=level_init(now_level_size, 1, LEVELING_WISCKEY, i);
				printf("LW[%d] - size:%u data:%.2lf(%%)\n",i, LSM.disk[i]->max_sst_num, 
						(double)LSM.disk[i]->max_sst_num*KP_IN_PAGE/RANGE*100);
			}
			else{
				LSM.disk[i]=level_init(now_level_size, 1, LEVELING, i);
				printf("L[%d] - size:%u data:%.2lf(%%)\n",i, LSM.disk[i]->max_sst_num, 
						(double)LSM.disk[i]->max_sst_num*KP_IN_PAGE/RANGE*100);	
			}
		}
		else{
			if(i<=LSM.param.tr.border_of_wisckey){
				EPRINT("tiering wisckey", true);
			}
			else{
				LSM.disk[i]=level_init(now_level_size, 1, TIERING, i);
				printf("TI[%d] - run_num:%u\n",i, LSM.disk[i]->max_run_num);
			}
		}
		now_level_size*=LSM.param.normal_size_factor;
		rwlock_init(&LSM.level_rwlock[i]);
	}

	LSM.last_run_version=version_init(LSM.param.tr.run_num, 
			LSM.param.last_size_factor, RANGE,
			LSM.disk,
			LSM.param.LEVELN);

	memset(all_set_page, -1, PAGESIZE);
	LSM.monitor.compaction_cnt=(uint32_t*)calloc(LSM.param.LEVELN+1, sizeof(uint32_t));
	slm_init(LSM.param.LEVELN);
	fdriver_mutex_init(&LSM.flush_lock);


	read_helper_prepare(
			LSM.param.bf_ptr_guard_rhp.target_prob, 
			LSM.param.bf_ptr_guard_rhp.member_num, 
			BLOOM_PTR_PAIR);

	read_helper_prepare(
			LSM.param.bf_guard_rhp.target_prob,
			LSM.param.bf_guard_rhp.member_num,
			BLOOM_ONLY);

	rwlock_init(&LSM.flushed_kp_set_lock);

	rwlock_init(&LSM.flush_wait_wb_lock);
	LSM.flush_wait_wb=NULL;

	LSM.gc_unavailable_seg=(uint32_t*)calloc(_NOS, sizeof(uint32_t));

	memset(LSM.now_merging_run, -1, sizeof(uint32_t)*(1+1));
	LSM.li=li;

	rb.pending_req=new std::map<uint32_t, algo_req *>();
	rb.issue_req=new std::map<uint32_t, algo_req*>();
	fdriver_mutex_init(&rb.pending_lock);
	fdriver_mutex_init(&rb.read_buffer_lock);
	rb.buffer_ppa=UINT32_MAX;

	lsmtree_monitor_init();
	return 1;
}

static void lsmtree_monitor_print(){

	printf("----LSMtree monitor log----\n");
	printf("TRIVIAL MOVE cnt:%u\n", LSM.monitor.trivial_move_cnt);
	for(uint32_t i=0; i<=LSM.param.LEVELN; i++){
		printf("COMPACTION %u cnt: %u\n", i, LSM.monitor.compaction_cnt[i]);
	}
	printf("COMPACTION_EARLY_INVALIDATION: %u\n", LSM.monitor.compaction_early_invalidation_cnt);
	printf("DATA GC cnt:%u\n", LSM.monitor.gc_data);
	printf("MAPPING GC cnt:%u\n", LSM.monitor.gc_mapping);

	printf("\n");
	printf("merge efficienty:%.2f (%lu/%lu - valid/total)\n", 
			(double)LSM.monitor.merge_valid_entry_cnt/LSM.monitor.merge_total_entry_cnt,
			LSM.monitor.merge_valid_entry_cnt,
			LSM.monitor.merge_total_entry_cnt);
	printf("tiering efficienty:%.2f (%lu/%lu - valid/total)\n", 
			(double)LSM.monitor.tiering_valid_entry_cnt/LSM.monitor.tiering_total_entry_cnt,
			LSM.monitor.tiering_valid_entry_cnt,
			LSM.monitor.tiering_total_entry_cnt);
	printf("\n");

	printf("level print\n");
	lsmtree_level_summary(&LSM);
	printf("\n");

	uint32_t version_number=LSM.last_run_version->total_version_number;
	uint64_t tiering_memory=0;
	uint64_t leveling_memory=0;
	uint64_t usage_bit=lsmtree_all_memory_usage(&LSM, &leveling_memory, &tiering_memory, 32) + RANGE*ceil(log2(version_number));
	uint64_t showing_size=RANGE*32/8; //for 48 bit
	//printf("sw size:%lu\n", showing_size);
	printf("[32-BIT] memory usage: %.2lf for PFTL\n\t%lu(bit)\n\t%.2lf(byte)\n\t%.2lf(MB)\n",
			(double)usage_bit/8/(showing_size),
			usage_bit,
			(double)usage_bit/8,
			(double)usage_bit/8/1024/1024
			);
	printf("  memory breakdown\n\tleveling:%lu (%.2lf)\n\ttiering:%lu (%.2lf)\n\tversion_bit:%lu (%.2lf)\n\n",
			leveling_memory, (double)leveling_memory/usage_bit,
			tiering_memory, (double)tiering_memory/usage_bit,
			(uint64_t)(RANGE*ceil(log2(version_number))), (double)RANGE*ceil(log2(version_number))/usage_bit	
			);

	tiering_memory=0;
	leveling_memory=0;
	usage_bit=lsmtree_all_memory_usage(&LSM, &leveling_memory, &tiering_memory, 48) + RANGE*ceil(log2(version_number));
	showing_size=RANGE*48/8; //for 48 bit
	printf("\n[48-BIT] memory usage: %.2lf for PFTL\n\t%lu(bit)\n\t%.2lf(byte)\n\t%.2lf(MB)\n",
			(double)usage_bit/8/(showing_size),
			usage_bit,
			(double)usage_bit/8,
			(double)usage_bit/8/1024/1024
			);
	printf("  memory breakdown\n\tleveling:%lu (%.2lf , %.3lf)\n\ttiering:%lu (%.2lf , %.3lf)\n\tversion_bit:%lu (%.2lf)\n\n",
			leveling_memory, (double)leveling_memory/usage_bit, (double)leveling_memory/(RANGE*48),
			tiering_memory, (double)tiering_memory/usage_bit, (double)tiering_memory/(RANGE*48),
			(uint64_t)(RANGE*ceil(log2(version_number))), (double)RANGE*ceil(log2(version_number))/usage_bit	
			);


	printf("----- Time result ------\n");
	print_adding_result("leveling RH [make] time :", 
			&LSM.monitor.RH_make_stopwatch[0], "\n");
	print_adding_result("Tiering RH [make] time :", 
			&LSM.monitor.RH_make_stopwatch[1], "\n");
	print_adding_result("leveling RH [check] time :", 
			&LSM.monitor.RH_check_stopwatch[0], "\n");
	print_adding_result("Tiering RH [check] time :", 
			&LSM.monitor.RH_check_stopwatch[1], "\n");

	printf("\n");
}

void lsmtree_destroy(lower_info *li, algorithm *){
	lsmtree_monitor_print();
	printf("----- traffic result -----\n");
	printf("RAF: %lf\n",
			(double)(li->req_type_cnt[DATAR]+li->req_type_cnt[MISSDATAR])/li->req_type_cnt[DATAR]);
	printf("WAF: %lf\n\n",
			(double)(li->req_type_cnt[MAPPINGW] +
				li->req_type_cnt[DATAW]+
				li->req_type_cnt[GCDW]+
				li->req_type_cnt[GCMW_DGC]+
				li->req_type_cnt[COMPACTIONDATAW])/li->req_type_cnt[DATAW]);
	compaction_free(LSM.cm);
	for(uint32_t i=0; i<WRITEBUFFER_NUM; i++){
		write_buffer_free(LSM.wb_array[i]);
	}
	for(uint32_t  i=0; i<LSM.param.LEVELN; i++){
		rwlock_destroy(&LSM.level_rwlock[i]);
	}	

	version_free(LSM.last_run_version);

	page_manager_free(LSM.pm);
	
	rwlock_destroy(&LSM.flushed_kp_set_lock);
	rwlock_destroy(&LSM.flush_wait_wb_lock);
	delete LSM.flushed_kp_set;

	free(LSM.gc_unavailable_seg);

	delete rb.pending_req;
	delete rb.issue_req;

	//lsmtree_tiered_level_all_print();
}

static inline algo_req *get_read_alreq(request *const req, uint8_t type, 
		uint32_t physical_address, lsmtree_read_param *r_param){
	algo_req *res=(algo_req *)calloc(1,sizeof(algo_req));
	res->ppa=physical_address;
	r_param->piece_ppa=physical_address;
	res->type=type;
	res->param=(void*)r_param;
	res->end_req=lsmtree_read_end_req;
	res->parents=req;
	return res;
}

static bool lsmtree_select_target_place(lsmtree_read_param *r_param, level **lptr, 
		run **rptr, sst_file **sptr, uint32_t lba){
retry:
	//r_param->prev_level++;
	if(r_param->prev_level==-1){
		r_param->prev_level=version_to_level_idx(LSM.last_run_version, 
				version_map_lba(LSM.last_run_version, lba),
				LSM.param.LEVELN);
	}
	if(r_param->prev_level>=LSM.param.LEVELN){
		return false;
	}

	if(r_param->prev_level==LSM.param.LEVELN-1){
		if(LSM.param.version_enable){
			if(r_param->prev_run==UINT32_MAX){
				r_param->prev_run=r_param->version;
			}
			else{
				return false;
			}
		}
		else{
			if(r_param->prev_run==UINT32_MAX){
				r_param->prev_run=LSM.disk[r_param->prev_level]->run_num;
			}
			else{
				if(r_param->prev_run==0){
					return false;
				}
				r_param->prev_run--;
			}
		}

		*lptr=LSM.disk[r_param->prev_level];
		*rptr=&(*lptr)->array[r_param->prev_run];
	}
	else{
		*lptr=LSM.disk[r_param->prev_level];
		*rptr=&(*lptr)->array[0];
	}

	if((*lptr)->level_type!=TIERING){
		*sptr=level_retrieve_sst(*lptr, lba);
	}
	else{
		if(r_param->prev_run > ((*lptr)->run_num)){
			printf("not find :%u ???\n", lba);
			EPRINT("not found eror!", true);
		}
		*sptr=run_retrieve_sst(*rptr, lba);
	}

	if(*sptr==NULL){
		goto retry;
	}
	r_param->prev_sf=*sptr;
	r_param->read_helper_idx=read_helper_idx_init((*sptr)->_read_helper, lba);
	r_param->rh=(*sptr)->_read_helper;
	return true;
}


void lsmtree_find_version_with_lock(uint32_t lba, lsmtree_read_param *param){
	rwlock *res;
	uint32_t version;
	uint32_t queried_level;
	for(int i=-1; i<(int)LSM.param.LEVELN; i++){
		if(i==-1 || i==0){
			res=&LSM.level_rwlock[0];	
		}
		else{
		//	rwlock_read_lock(LSM.level_rwlock[i-1]);
			res=&LSM.level_rwlock[i];
		}
		rwlock_read_lock(res);
		version=version_map_lba(LSM.last_run_version, lba);
		queried_level=version_to_level_idx(LSM.last_run_version, version, LSM.param.LEVELN);
		if((i==-1 && queried_level) || (i==queried_level)){
			param->prev_level=queried_level;
			param->version=version;
			param->target_level_rw_lock=res;
		}
		else{
			rwlock_read_unlock(res);
		}
	}
}

static void not_found_process(request *const req){
	lsmtree_read_param * r_param=(lsmtree_read_param*)req->param;
	if(r_param->target_level_rw_lock){
		rwlock_read_unlock(r_param->target_level_rw_lock);
	}
	printf("req->key: %u-", req->key);
	EPRINT("not found key", false);
	printf("prev_level:%u prev_run:%u read_helper_idx:%u version:%u\n", 
			r_param->prev_level, r_param->prev_run, r_param->read_helper_idx, version_map_lba(LSM.last_run_version,req->key));

	LSM_find_lba(&LSM, req->key);
	abort();
	req->type=FS_NOTFOUND_T;
	req->end_req(req);
}

enum{
	BUFFER_HIT, BUFFER_MISS, BUFFER_PENDING
};

static inline uint32_t read_buffer_checker(uint32_t ppa, value_set *value, algo_req *req, bool sync){
	if(req->type==DATAR){
		fdriver_lock(&rb.read_buffer_lock);
		if(ppa==rb.buffer_ppa){
			processing_data_read_req(req, rb.buffer_value, false);
			fdriver_unlock(&rb.read_buffer_lock);
			return BUFFER_HIT;
		}
		else{
			fdriver_unlock(&rb.read_buffer_lock);
		}

		fdriver_lock(&rb.pending_lock);
		rb_r_iter temp_r_iter=rb.issue_req->find(ppa);
		if(temp_r_iter==rb.issue_req->end()){
			rb.issue_req->insert(std::pair<uint32_t,algo_req*>(ppa, req));
			fdriver_unlock(&rb.pending_lock);
		}
		else{
			rb.pending_req->insert(std::pair<uint32_t, algo_req*>(ppa, req));
			fdriver_unlock(&rb.pending_lock);
			return BUFFER_PENDING;
		}
	}
	

	io_manager_issue_read(ppa, value, req, sync);
	return BUFFER_MISS;
}


uint32_t lsmtree_read(request *const req){
	lsmtree_read_param *r_param;
	if(req->key==debug_lba){
		printf("req->key:%u\n", req->key);
	}
	if(!req->param){
		r_param=(lsmtree_read_param*)calloc(1, sizeof(lsmtree_read_param));
		req->param=(void*)r_param;
		r_param->prev_level=-1;
		r_param->prev_run=UINT32_MAX;
		r_param->target_level_rw_lock=NULL;
		r_param->piece_ppa=UINT32_MAX;

		/*find data from write_buffer*/
		lsmtree_find_version_with_lock(req->key, r_param);

		r_param->target_level_rw_lock=&LSM.level_rwlock[0];
		rwlock_read_lock(r_param->target_level_rw_lock);
		char *target;
		for(uint32_t i=0; i<WRITEBUFFER_NUM; i++){
			if((target=write_buffer_get(LSM.wb_array[i], req->key))){
				//	printf("find in write_buffer");
				memcpy(req->value->value, target, LPAGESIZE);
				rwlock_read_unlock(r_param->target_level_rw_lock);
				free(r_param);
				req->end_req(req);
				return 1;
			}
		}

		rwlock_read_lock(&LSM.flush_wait_wb_lock);
		if(LSM.flush_wait_wb && (target=write_buffer_get(LSM.flush_wait_wb, req->key))){
			//	printf("find in write_buffer");
			memcpy(req->value->value, target, LPAGESIZE);
			rwlock_read_unlock(r_param->target_level_rw_lock);
			free(r_param);

			req->end_req(req);
			rwlock_read_unlock(&LSM.flush_wait_wb_lock);
			return 1;
		}
		rwlock_read_unlock(&LSM.flush_wait_wb_lock);

		uint32_t target_piece_ppa=UINT32_MAX;
		
		if(LSM.flushed_kp_set){
			rwlock_read_lock(&LSM.flushed_kp_set_lock);
			std::map<uint32_t, uint32_t>::iterator iter=LSM.flushed_kp_set->find(req->key);
			if(iter!=LSM.flushed_kp_set->end()){
				rwlock_read_unlock(r_param->target_level_rw_lock);//L1 unlock
				target_piece_ppa=iter->second;
				r_param->target_level_rw_lock=NULL;
				rwlock_read_unlock(&LSM.flushed_kp_set_lock);
				algo_req *alreq=get_read_alreq(req, DATAR, target_piece_ppa, r_param);
				read_buffer_checker(PIECETOPPA(target_piece_ppa), req->value, alreq, false);
				return 1;
			}
			else{
				rwlock_read_unlock(&LSM.flushed_kp_set_lock);
			}
		}
	}
	else{
		r_param=(lsmtree_read_param*)req->param;
	}

	algo_req *al_req;
	level *target=NULL;
	run *rptr=NULL;
	sst_file *sptr=NULL;
	uint32_t read_target_ppa=r_param->piece_ppa; //map_ppa && map_piece_ppa
	//uint32_t read_target_ppa=UINT32_MAX; //map_ppa && map_piece_ppa

	switch(r_param->check_type){
		case K2VMAP:
			read_target_ppa=kp_find_piece_ppa(req->key, req->value->value);
			if(read_target_ppa==UINT32_MAX){//not found
				r_param->check_type=NOCHECK;
			}
			else{//FOUND
				al_req=get_read_alreq(req, DATAR, read_target_ppa, r_param);
				read_buffer_checker(PIECETOPPA(read_target_ppa), req->value, al_req, false);
				goto normal_end;
			}
			break;
		case DATA:
			break;
		case PLR:
			break;
		case NOCHECK:
			break;
	}
	
retry:
	/*
	if((target && rptr) &&
		(r_param->prev_level==LSM.param.LEVELN-1)&& 
		(&target->array[target->run_num]==rptr)){
		goto notfound;
	}*/

	if(r_param->use_read_helper){
		if(!read_helper_last(r_param->prev_sf->_read_helper, r_param->read_helper_idx)){
			sptr=r_param->prev_sf;
			goto read_helper_check_again;
		}
	}
	
	if(lsmtree_select_target_place(r_param, &target, &rptr, &sptr, req->key)==false){
		goto notfound;
	}
		
	if(!sptr){
		goto retry;
	}
	
read_helper_check_again:

	if(sptr->_read_helper){
		/*issue data read!*/
		r_param->use_read_helper=true;
		if(read_helper_check(sptr->_read_helper, req->key, &read_target_ppa, sptr, &r_param->read_helper_idx)){
			al_req=get_read_alreq(req, DATAR, read_target_ppa, r_param);
			read_buffer_checker(PIECETOPPA(read_target_ppa), req->value, al_req, false);
			goto normal_end;
		}else{
			if(r_param->read_helper_idx==UINT32_MAX){
				goto notfound;
			}
			if(lsmtree_select_target_place(r_param, &target, &rptr, &sptr, req->key)==false){
				goto notfound;
			}
			goto retry;
		}
	}
	else{
		/*issue map read!*/
		read_target_ppa=target->level_type==TIERING?
			sst_find_map_addr(sptr, req->key):sptr->file_addr.map_ppa;
		if(read_target_ppa==UINT32_MAX){
			goto notfound;
		}
		r_param->check_type=K2VMAP;
		al_req=get_read_alreq(req, MAPPINGR, read_target_ppa, r_param);
		read_buffer_checker(read_target_ppa, req->value, al_req, false);
		goto normal_end;
	}

notfound:
	not_found_process(req);
	return 0;

normal_end:
	return 1;
}


uint32_t lsmtree_write(request *const req){
	write_buffer *wb=LSM.wb_array[LSM.now_wb];
	write_buffer_insert(wb, req->key, req->value);

	if(write_buffer_isfull(wb)){
retry:
		rwlock_write_lock(&LSM.flush_wait_wb_lock);
		if(LSM.flush_wait_wb){
			//EPRINT("compactino not done!", false);
			rwlock_write_unlock(&LSM.flush_wait_wb_lock);
			goto retry;
		}
		LSM.flush_wait_wb=LSM.wb_array[LSM.now_wb];
		rwlock_write_unlock(&LSM.flush_wait_wb_lock);

		compaction_req *temp_req=MAKE_L0COMP_REQ(wb, NULL, false);
		compaction_issue_req(LSM.cm, temp_req);


		LSM.wb_array[LSM.now_wb]=write_buffer_init(KP_IN_PAGE-L2PGAP, LSM.pm, NORMAL_WB);
		if(++LSM.now_wb==WRITEBUFFER_NUM){
			LSM.now_wb=0;
		}
	}

	req->value=NULL;
	req->end_req(req);
	return 1;
}


void lsmtree_compaction_end_req(compaction_req* req){
	free(req);
}

uint32_t lsmtree_flush(request *const req){
	printf("not implemented!!\n");
	return 1;
}

uint32_t lsmtree_remove(request *const req){
	printf("not implemented!!\n");
	return 1;
}

static void processing_data_read_req(algo_req *req, char *v, bool from_end_req_path){
	request *parents=req->parents;
	lsmtree_read_param *r_param=(lsmtree_read_param*)req->param;
	uint32_t offset;
	uint32_t piece_ppa=req->ppa;

	if(r_param->use_read_helper && 
			read_helper_data_checking(r_param->rh, LSM.pm, piece_ppa, parents->key, &r_param->read_helper_idx,&offset, r_param->prev_sf)){

		if(r_param->target_level_rw_lock){
			rwlock_read_unlock(r_param->target_level_rw_lock);
		}
		if(offset>=L2PGAP){
			EPRINT("can't be plz checking oob_lba_checker", true);
		}
		memcpy(parents->value->value, &v[offset*LPAGESIZE], LPAGESIZE);
		parents->end_req(parents);
		free(r_param);
		free(req);
	}
	else if(page_manager_oob_lba_checker(LSM.pm, piece_ppa, 
				parents->key, &offset)){
		if(r_param->target_level_rw_lock){
			rwlock_read_unlock(r_param->target_level_rw_lock);
		}

		if(offset>=L2PGAP){
			EPRINT("can't be plz checking oob_lba_checker", true);
		}
		memcpy(parents->value->value, &v[offset*LPAGESIZE], LPAGESIZE);

		parents->end_req(parents);
		free(r_param);
		free(req);
	}
	else{
		if(from_end_req_path){
			LSM.li->req_type_cnt[DATAR]--;
			LSM.li->req_type_cnt[MISSDATAR]++;
			parents->type_ftl++;
		}
		if(!inf_assign_try(parents)){
			EPRINT("why cannot retry?", true);
		}	
	}
}

void *lsmtree_read_end_req(algo_req *req){
	rb_r_iter target_r_iter;
	request *parents=req->parents;
	algo_req *pending_req;
	uint32_t type=req->type;
	switch(type){
		case MAPPINGR:
			parents->type_ftl++;
			if(!inf_assign_try(parents)){
				EPRINT("why cannot retry?", true);
			}
			break;
		case DATAR:
			fdriver_lock(&rb.read_buffer_lock);
			rb.buffer_ppa=PIECETOPPA(req->ppa);
			memcpy(rb.buffer_value, parents->value->value, PAGESIZE);
			fdriver_unlock(&rb.read_buffer_lock);

			fdriver_lock(&rb.pending_lock);
			target_r_iter=rb.pending_req->find(PIECETOPPA(req->ppa));
			for(;target_r_iter->first==PIECETOPPA(req->ppa) && 
					target_r_iter!=rb.pending_req->end();){
				pending_req=target_r_iter->second;
				processing_data_read_req(pending_req, parents->value->value, false);
				rb.pending_req->erase(target_r_iter++);
			}
			rb.issue_req->erase(req->ppa/L2PGAP);
			processing_data_read_req(req, parents->value->value, true);
			fdriver_unlock(&rb.pending_lock);
			break;
	}
	if(type==MAPPINGR){
		free(req);
	}
	return NULL;
}


void lsmtree_level_summary(lsmtree *lsm){
	for(uint32_t i=0; i<lsm->param.LEVELN; i++){
		printf("ptr:%p ", lsm->disk[i]); level_print(lsm->disk[i]);
	}
}

void lsmtree_content_print(lsmtree *lsm){
	for(uint32_t i=0; i<lsm->param.LEVELN; i++){
		level_content_print(lsm->disk[i], true);
	}
}
sst_file *lsmtree_find_target_sst_mapgc(uint32_t lba, uint32_t map_ppa){
	sst_file *res=NULL;
	for(uint32_t i=0; i<LSM.param.LEVELN-1; i++){
		level *lev=LSM.disk[i];
		res=level_retrieve_sst(lev, lba);
		if(!res) continue;
		if(lba==res->start_lba && res->file_addr.map_ppa==map_ppa){
			return res;
		}
	}
	EPRINT("not found target", true);
	return res;
}

void lsmtree_gc_unavailable_set(lsmtree *lsm, sst_file *sptr, uint32_t seg_idx){
	if(sptr){
		lsm->gc_unavailable_seg[sptr->end_ppa/_PPS]++;
	}else{
		lsm->gc_unavailable_seg[seg_idx]++;
	}
}

void lsmtree_gc_unavailable_unset(lsmtree *lsm, sst_file *sptr, uint32_t seg_idx){
	if(sptr){
		lsm->gc_unavailable_seg[sptr->end_ppa/_PPS]--;
	}else{
		lsm->gc_unavailable_seg[seg_idx]--;
	}
}

void lsmtree_gc_unavailable_sanity_check(lsmtree *lsm){
	EPRINT("remove this code for exp", false);
	for(uint32_t i=0; i<_NOS; i++){
		if(lsm->gc_unavailable_seg[i]){
			EPRINT("should be zero", true);
		}
	}
}

uint64_t lsmtree_all_memory_usage(lsmtree *lsm, uint64_t *leveling, uint64_t *tiering, uint32_t lba_unit){
	uint64_t bit=0;
	(*leveling)=0;
	(*tiering)=0;
	for(uint32_t i=0; i<lsm->param.LEVELN; i++){
		level *lev=lsm->disk[i];
		run *rptr;
		uint32_t ridx;
		for_each_run_max(lev, rptr, ridx){
			if(!rptr->now_sst_file_num) continue;
			sst_file *sptr;
			uint32_t sidx;
			uint32_t now_bit;
			for_each_sst(rptr, sptr, sidx){
				if(sptr->_read_helper){
					now_bit=read_helper_memory_usage(sptr->_read_helper, lba_unit);
					if(lev->level_type==TIERING){
						(*tiering)+=now_bit;
					}
					else{
						(*leveling)+=now_bit;
					}
					bit+=now_bit;
				}
			}	
		}
	}
	return bit;
}

void lsmtree_tiered_level_all_print(){
	level *disk=LSM.disk[LSM.param.LEVELN-1];
	run *rptr; uint32_t r_idx;
	for_each_run(disk, rptr, r_idx){
		sst_file *sptr; uint32_t s_idx;
		for_each_sst(rptr, sptr, s_idx){
			char map_data[PAGESIZE];
			map_range *mr;
			for(uint32_t mr_idx=0; mr_idx<sptr->map_num; mr_idx++){
				mr=&sptr->block_file_map[mr_idx];
				io_manager_test_read(mr->ppa, map_data, TEST_IO);
				key_ptr_pair* kp_set=(key_ptr_pair*)map_data;
				for(uint32_t i=0; i<KP_IN_PAGE && kp_set[i].lba!=UINT32_MAX; i++){
					dprintf(tiered_level_fd,"%u %u\n",kp_set[i].piece_ppa, kp_set[i].lba);
				}
			}
		}
		dprintf(tiered_level_fd, "\n");
	}
}
typedef std::unordered_map<uint32_t, slm_node*> seg_map;
typedef std::unordered_map<uint32_t, slm_node*>::iterator seg_map_iter;
typedef std::pair<uint32_t, slm_node*> seg_map_pair;

void lsmtree_gc_lock_level(lsmtree *lsm, uint32_t level_idx){
	seg_map *t_map=slm_get_target_map(level_idx);
	seg_map_iter t_iter=t_map->begin();

	for(; t_iter!=t_map->end(); t_iter++){
		lsmtree_gc_unavailable_set(lsm, NULL, t_iter->second->seg_idx);
	}
}

void lsmtree_gc_unlock_level(lsmtree *lsm, uint32_t level_idx){
	seg_map *t_map=slm_get_target_map(level_idx);
	seg_map_iter t_iter=t_map->begin();

	for(; t_iter!=t_map->end(); t_iter++){
		lsmtree_gc_unavailable_unset(lsm, NULL, t_iter->second->seg_idx);
	}
}

read_helper_param lsmtree_get_target_rhp(uint32_t level_idx){
	if(level_idx<=LSM.param.tr.border_of_bf){
		if(level_idx<=LSM.param.tr.border_of_wisckey){
			return LSM.param.bf_ptr_guard_rhp;
		}
		else{
			return LSM.param.bf_guard_rhp;
		}
	}
	else{
		if(level_idx<=LSM.param.tr.border_of_wisckey){
			EPRINT("?????", true);
		}
		else{
			return LSM.param.plr_rhp;
		}
	}
	return LSM.param.plr_rhp;
}
