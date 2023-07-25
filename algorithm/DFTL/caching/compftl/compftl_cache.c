#include "compftl_cache.h"
//#include "bitmap_ops.h"
#include "../../demand_mapping.h"
#include "../../../../include/settings.h"
#include <stdio.h>
#include <stdlib.h>
#include "../../../../include/debug_utils.h"

extern uint32_t debug_lba;
extern uint32_t test_ppa;
//bool global_debug_flag=false;

my_cache compftl_cache_func{
	.init=compftl_init,
	.free=compftl_free,
	.is_needed_eviction=compftl_is_needed_eviction,
	.need_more_eviction=compftl_is_needed_eviction,
	.update_eviction_hint=compftl_update_eviction_hint,
	.is_hit_eviction=compftl_is_hit_eviction,
	.update_hit_eviction_hint=compftl_update_hit_eviction_hint,
	.is_eviction_hint_full=compftl_is_eviction_hint_full,
	.get_remain_space=compftl_get_remain_space,
	.update_entry=compftl_update_entry,
	.update_entry_gc=compftl_update_entry_gc,
	.force_put_mru=compftl_force_put_mru,
	.insert_entry_from_translation=compftl_insert_entry_from_translation,
	.update_from_translation_gc=compftl_update_from_translation_gc,
	.get_mapping=compftl_get_mapping,
	.get_eviction_GTD_entry=compftl_get_eviction_GTD_entry,
	.get_eviction_mapping_entry=NULL,
	.update_eviction_target_translation=compftl_update_eviction_target_translation,
	.evict_target=NULL,
	.dump_cache_update=compftl_dump_cache_update,
	.load_specialized_meta=compftl_load_specialized_meta,
	.update_dynamic_size=compftl_update_dynamic_size,
	.empty_cache=compftl_empty_cache,
	.exist=compftl_exist,
	.print_log=NULL,
};

compftl_cache_monitor ccm;
extern demand_map_manager dmm;

uint32_t compftl_init(struct my_cache *mc, uint32_t total_caching_physical_pages){
	lru_init(&ccm.lru, NULL, NULL);
	uint32_t half_translate_page_num=RANGE/(PAGESIZE/(sizeof(uint32_t)))/2;
	ccm.max_caching_byte=total_caching_physical_pages * PAGESIZE - half_translate_page_num * (4+4);
	ccm.max_caching_byte-=half_translate_page_num*2/8; //for dirty bit
	ccm.now_caching_byte=0;
	mc->type=COARSE;
	mc->entry_type=DYNAMIC;
	mc->private_data=NULL;
	ccm.gtd_size=(uint32_t*)malloc(GTDNUM *sizeof(uint32_t));
	for(uint32_t i=0; i<GTDNUM; i++){
		ccm.gtd_size[i]=BITMAPSIZE+PAGESIZE;
	}

	printf("|\tcaching <min> percentage: %.2lf%%\n", (double) ((ccm.max_caching_byte/(BITMAPSIZE+PAGESIZE)) * BITMAPMEMBER)/ RANGE *100);
	return (ccm.max_caching_byte/(BITMAPSIZE+sizeof(uint32_t))) * BITMAPMEMBER;
}

uint32_t compftl_free(struct my_cache *mc){
	uint32_t total_entry_num=0;
	while(1){
		compftl_cache *cc=(compftl_cache*)lru_pop(ccm.lru);
		if(!cc) break;
		uint32_t total_head=(ccm.gtd_size[cc->etr->idx]-BITMAPSIZE)/sizeof(uint32_t);
	//	printf("[%u] %u\n", cc->etr->idx, total_head);
		total_entry_num+=PAGESIZE/sizeof(DMF);
		free(cc->head_array);
		bitmap_free(cc->map);
		free(cc);
	}
	printf("now byte:%u max_byte:%u\n", ccm.now_caching_byte, ccm.max_caching_byte);
	printf("cached_entry_num:%u (%lf)\n", total_entry_num, (double)total_entry_num/RANGE);

	uint32_t average_head_num=0;
	uint32_t head_histogram[PAGESIZE/sizeof(uint32_t) + 1]={0,};
	for(uint32_t i=0; i<GTDNUM; i++){
		uint32_t temp_head_num=(ccm.gtd_size[i]-BITMAPSIZE)/sizeof(uint32_t);
	//	pritnf("%u -> %u\n", i, temp_head_num);
		head_histogram[temp_head_num]++;
		average_head_num+=temp_head_num;
	}
	printf("average head num:%lf\n", (double)(average_head_num)/GTDNUM);
	
	for(uint32_t i=0; i<=PAGESIZE/sizeof(uint32_t); i++){
		if(head_histogram[i]){
			printf("%u,%u\n", i, head_histogram[i]);
		}
	}

	lru_free(ccm.lru);
	free(ccm.gtd_size);
	return 1;
}

uint32_t compftl_is_needed_eviction(struct my_cache *mc, uint32_t lba, uint32_t *, uint32_t eviction_hint){
	uint32_t target_size=ccm.gtd_size[GETGTDIDX(lba)];
	if(ccm.max_caching_byte <= ccm.now_caching_byte+target_size+sizeof(uint32_t)*2+(eviction_hint)){
		return ccm.now_caching_byte==0? EMPTY_EVICTION : NORMAL_EVICTION;
	}

	/*compftl get eviction gtd entry*/
	/*
	lru_node *target;
	GTD_entry *etr=NULL;
	for_each_lru_backword(ccm.lru, target){
		compftl_cache *cc=(compftl_cache*)target->data;
		etr=cc->etr;
		if(target_size > ccm.gtd_size[etr->idx]){
			break;
		}
	}*/

	if(ccm.max_caching_byte <= ccm.now_caching_byte){
		/*
		printf("now caching byte bigger!!!! %s:%d\n", __FILE__, __LINE__);
		abort();*/
	}
	return HAVE_SPACE;
}

uint32_t compftl_update_eviction_hint(struct my_cache *, uint32_t lba, uint32_t * /*prefetching_info*/,uint32_t eviction_hint, 
		uint32_t *now_eviction_hint, bool increase){
	uint32_t target_size=ccm.gtd_size[GETGTDIDX(lba)];
	if(increase){
		*now_eviction_hint=target_size+sizeof(uint32_t)*2;
		return eviction_hint+*now_eviction_hint;
	}
	else{
		return eviction_hint-*now_eviction_hint;
	}
}

inline static compftl_cache* get_initial_state_cache(uint32_t gtd_idx, GTD_entry *etr){
	compftl_cache *res=(compftl_cache *)malloc(sizeof(compftl_cache));
	res->head_array=(uint32_t*)malloc(PAGESIZE);
	memset(res->head_array, -1, PAGESIZE);
	res->map=bitamp_set_init(BITMAPMEMBER);
	res->etr=etr;
	
	ccm.gtd_size[gtd_idx]=PAGESIZE+BITMAPSIZE;
	
	return res;
}

inline static uint32_t get_ppa_from_cc(compftl_cache *cc, uint32_t lba){
	uint32_t head_offset=get_head_offset(cc->map, lba);
	uint32_t distance=get_distance(cc->map, lba);
	return cc->head_array[head_offset]+distance;
}

inline static bool is_sequential(compftl_cache *cc, uint32_t lba, uint32_t ppa){
	uint32_t offset=GETOFFSET(lba);
	if(offset==0) return false;

	uint32_t head_offset=get_head_offset(cc->map, lba-1);
	uint32_t distance=0;
	if(!bitmap_is_set(cc->map, GETOFFSET(lba-1))){
		distance=get_distance(cc->map, lba-1);
	}
	
	if(cc->head_array[head_offset]+distance+1==ppa){
		if(cc->head_array[head_offset]==UINT32_MAX) return false;
		return true;
	}
	else return false;
}

static inline void compftl_size_checker(uint32_t eviction_hint){
	if(ccm.now_caching_byte+eviction_hint> ccm.max_caching_byte/100*110){
		printf("n:%u m:%u e:%ucaching overflow! %s:%d\n", ccm.now_caching_byte, ccm.max_caching_byte, eviction_hint, __FILE__, __LINE__);
		abort();
	}
}

enum NEXT_STEP{
	DONE, SHRINK, EXPAND
};

inline static uint32_t shrink_cache(compftl_cache *cc, uint32_t lba, uint32_t ppa, char *should_more, uint32_t *more_ppa){

	bool is_next_do=false;
	uint32_t next_original_ppa;

	if(!ISLASTOFFSET(lba)){
		if(!bitmap_is_set(cc->map, GETOFFSET(lba+1))){
			is_next_do=true;
			next_original_ppa=get_ppa_from_cc(cc, lba+1);
		}
	}

	uint32_t old_ppa;
	if(bitmap_is_set(cc->map, GETOFFSET(lba))){
		uint32_t head_offset=get_head_offset(cc->map, lba);
		old_ppa=cc->head_array[head_offset];

		static int cnt=0;
		uint32_t total_head=(ccm.gtd_size[GETGTDIDX(lba)]-BITMAPSIZE)/sizeof(uint32_t);
		uint32_t *new_head_array=(uint32_t*)malloc((total_head-1+(is_next_do?1:0))*sizeof(uint32_t));
	
		//printf("test cnt:%u %u %u %u %u\n", cnt, total_head, head_offset, lba, *should_more);
		memcpy(new_head_array, cc->head_array, (head_offset)*sizeof(uint32_t));
		if(is_next_do){
			bitmap_set(cc->map, GETOFFSET(lba+1));
			new_head_array[head_offset]=next_original_ppa;
		}


		//printf("test cnt2:%u\n", cnt++);
		if(total_head!=head_offset+(is_next_do?1:0)){
			memcpy(&new_head_array[head_offset+(is_next_do?1:0)], &cc->head_array[(head_offset+1)], (total_head-1-head_offset)*sizeof(uint32_t));
		}
		free(cc->head_array);
		cc->head_array=new_head_array;
		ccm.gtd_size[GETGTDIDX(lba)]=(total_head-1+(is_next_do?1:0))*sizeof(uint32_t)+BITMAPSIZE;

		
		bitmap_unset(cc->map, GETOFFSET(lba));
	}
	else{
		printf("it cannot be sequential with previous ppa %s:%d\n", __FILE__, __LINE__);
		abort();
	}

	if(!ISLASTOFFSET(lba)){
		if(GETOFFSET(lba+1)< PAGESIZE/sizeof(uint32_t) ){
			if(bitmap_is_set(cc->map, GETOFFSET(lba+1))){
				uint32_t next_ppa=get_ppa_from_cc(cc, lba+1);
				if(ppa+1==next_ppa){
					*should_more=SHRINK;
					*more_ppa=next_ppa;
				}
				else{
					*should_more=DONE;
				}
			}
			else{
				*should_more=EXPAND;
				*more_ppa=get_ppa_from_cc(cc, lba+1);
			}
		}
		else{
			*should_more=DONE;
		}
	}
	else{
		*should_more=DONE;
	}

	return old_ppa;
}

inline static uint32_t expand_cache(compftl_cache *cc, uint32_t lba, uint32_t ppa, char *should_more, uint32_t *more_ppa){
	uint32_t old_ppa;	
	uint32_t head_offset;
	uint32_t distance=0;

	if(lba==5275648 && ppa==UINT32_MAX){
		//GDB_MAKE_BREAKPOINT;
	}

	head_offset=get_head_offset(cc->map, lba);

	bool is_next_do=false;
	uint32_t next_original_ppa;

	//DEBUG_CNT_PRINT(test, 55622396, __FUNCTION__, __LINE__);
	if(ISLASTOFFSET(lba)){}
	else{
		if(!bitmap_is_set(cc->map, GETOFFSET(lba+1))){
			is_next_do=true;
			next_original_ppa=get_ppa_from_cc(cc, lba+1);
		}
	}

	if(!bitmap_is_set(cc->map, GETOFFSET(lba))){

		distance=get_distance(cc->map, lba);
		old_ppa=cc->head_array[head_offset]+distance;

		uint32_t total_head=(ccm.gtd_size[GETGTDIDX(lba)]-BITMAPSIZE)/sizeof(uint32_t);
		uint32_t *new_head_array=(uint32_t*)malloc((total_head+1+(is_next_do?1:0))*sizeof(uint32_t));

		memcpy(new_head_array, cc->head_array, (head_offset+1) * sizeof(uint32_t));
		new_head_array[head_offset+1]=ppa;

		if(is_next_do){
			bitmap_set(cc->map, GETOFFSET(lba+1));
			new_head_array[head_offset+2]=next_original_ppa;
			if(total_head!=head_offset+1){
				memcpy(&new_head_array[head_offset+3], &cc->head_array[head_offset+1], (total_head-(head_offset+1)) * sizeof(uint32_t));
			}
		}
		else{
			if(total_head!=head_offset+1){
				memcpy(&new_head_array[head_offset+2], &cc->head_array[head_offset+1], (total_head-(head_offset+1)) * sizeof(uint32_t));
			}
		}
		free(cc->head_array);
		cc->head_array=new_head_array;
		ccm.gtd_size[GETGTDIDX(lba)]=(total_head+1+(is_next_do?1:0))*sizeof(uint32_t)+BITMAPSIZE;
		bitmap_set(cc->map, GETOFFSET(lba));
	//	compftl_print_mapping(cc);
	}
	else{
		old_ppa=cc->head_array[head_offset];
		cc->head_array[head_offset]=ppa;
	
		if(is_next_do){
			uint32_t total_head=(ccm.gtd_size[GETGTDIDX(lba)]-BITMAPSIZE)/sizeof(uint32_t);
			uint32_t *new_head_array=(uint32_t*)malloc((total_head+1)*sizeof(uint32_t));

			memcpy(new_head_array, cc->head_array, (head_offset+1) * sizeof(uint32_t));
			new_head_array[head_offset+1]=next_original_ppa;
			bitmap_set(cc->map, GETOFFSET(lba+1));

			if(total_head!=head_offset+1){
				memcpy(&new_head_array[head_offset+2], &cc->head_array[head_offset+1], (total_head-(head_offset+1)) * sizeof(uint32_t));
			}
			free(cc->head_array);
			cc->head_array=new_head_array;
			ccm.gtd_size[GETGTDIDX(lba)]=(total_head+1)*sizeof(uint32_t)+BITMAPSIZE;
			bitmap_set(cc->map, GETOFFSET(lba));
		}
		else{
			bitmap_set(cc->map, GETOFFSET(lba));
		}
	}

	if(GETOFFSET(lba+1)< PAGESIZE/sizeof(uint32_t) && !ISLASTOFFSET(lba)){
		if(bitmap_is_set(cc->map, GETOFFSET(lba+1))){

			*more_ppa=get_ppa_from_cc(cc, lba+1);
			if(ppa+1==(*more_ppa)){
				*should_more=SHRINK;
			}
			else{
				*should_more=DONE;
			}
		}
		else{
			*more_ppa=get_ppa_from_cc(cc, lba+1);
			*should_more=EXPAND;	
		}
	}
	else{
		*should_more=DONE;
	}

	return old_ppa;
}


inline static uint32_t __update_entry(GTD_entry *etr, uint32_t lba, uint32_t ppa, bool isgc, uint32_t *eviction_hint){
	compftl_cache *cc;

	uint32_t old_ppa;
	uint32_t gtd_idx=GETGTDIDX(lba);
	int32_t prev_gtd_size;
	int32_t changed_gtd_size;
	lru_node *ln;

	if(etr->status==EMPTY){
		cc=get_initial_state_cache(gtd_idx, etr);
		ln=lru_push(ccm.lru, cc);
		etr->private_data=(void*)ln;
	}else{
		if(ccm.now_caching_byte <= ccm.gtd_size[gtd_idx]){
			ccm.now_caching_byte=0;
		}
		else{
			ccm.now_caching_byte-=ccm.gtd_size[gtd_idx];
		}
		if(etr->private_data==NULL){
			printf("insert translation page before cache update! %s:%d\n",__FILE__, __LINE__);
			//print_stacktrace();
			abort();
		}
		ln=(lru_node*)etr->private_data;
		cc=(compftl_cache*)(ln->data);
	}

	prev_gtd_size=ccm.gtd_size[gtd_idx];

	uint32_t more_lba=lba;
	uint32_t more_ppa;
	char should_more=false;
	if(is_sequential(cc, lba, ppa)){
		old_ppa=shrink_cache(cc, lba, ppa, &should_more, &more_ppa);
	}
	else{
		old_ppa=expand_cache(cc, lba, ppa, &should_more, &more_ppa);
	}

	while(should_more!=DONE){
		more_lba++;
		switch(should_more){
			case SHRINK:
				shrink_cache(cc, more_lba, more_ppa, &should_more, &more_ppa);
				break;
			case EXPAND:
				expand_cache(cc, more_lba, more_ppa, &should_more, &more_ppa);
				break;
			default:
				break;
		}
	}
/*
	if(etr->idx==535){
		compftl_mapping_verify(cc);
		compftl_print_mapping(cc);
		printf("pair: %u, %u physical:%u\n", lba, ppa, etr->physical_address);
	}
	
*/
	//compftl_mapping_verify(cc);

	changed_gtd_size=ccm.gtd_size[gtd_idx];
	if(changed_gtd_size - prev_gtd_size > (int)sizeof(uint32_t)*2){
		printf("what happen???\n");
		abort();
	}
/*
	if(lba==2129921){//GETGTDIDX(lba)==520){
		printf("after %u-%u:", lba, ppa);
		compftl_print_mapping(cc);
		compftl_mapping_verify(cc);
	}
*/
	if((ccm.gtd_size[gtd_idx]-BITMAPSIZE)/sizeof(uint32_t) > PAGESIZE/sizeof(uint32_t)){
		printf("oversize!\n");
		compftl_print_mapping(cc);
		abort();
	}
	ccm.now_caching_byte+=ccm.gtd_size[gtd_idx];
	
	if(eviction_hint){
		compftl_size_checker(*eviction_hint);
	}
	else{
		compftl_size_checker(0);
	}

	if(!isgc){
		lru_update(ccm.lru, ln);
	}
	etr->status=DIRTY;
	return old_ppa;
}
extern uint32_t test_ppa;
uint32_t compftl_update_entry(struct my_cache *, GTD_entry *etr, uint32_t lba, uint32_t ppa, uint32_t *eviction_hint){
	return __update_entry(etr, lba, ppa, false, eviction_hint);
}

uint32_t compftl_update_entry_gc(struct my_cache *, GTD_entry *etr, uint32_t lba, uint32_t ppa){
	return __update_entry(etr, lba, ppa, true, NULL);
}

static inline compftl_cache *make_cc_from_translation(GTD_entry *etr, char *data){
	compftl_cache *cc=(compftl_cache*)malloc(sizeof(compftl_cache));
	cc->map=bitmap_init(BITMAPMEMBER);
	cc->etr=etr;

	uint32_t total_head=(ccm.gtd_size[etr->idx]-BITMAPSIZE)/sizeof(uint32_t);
	uint32_t *new_head_array=(uint32_t*)malloc(total_head * sizeof(uint32_t));
	uint32_t *ppa_list=(uint32_t*)data;
	uint32_t last_ppa, new_head_idx=0;
	bool sequential_flag=false;

	for(uint32_t i=0; i<PAGESIZE/sizeof(uint32_t); i++){
		if(new_head_idx>total_head){
			printf("total_head cannot smaller then new_head_idx %s:%d\n", __FILE__, __LINE__);
			abort();
		}
		if(i==0){
			last_ppa=ppa_list[i];
			new_head_array[new_head_idx++]=last_ppa;
			bitmap_set(cc->map, i);
		}
		else{
			sequential_flag=false;
			if((last_ppa!=UINT32_MAX) && last_ppa+1==ppa_list[i]){
				sequential_flag=true;
			}

			if(sequential_flag){
				if(cc->etr->idx==5275648/4096 && i==0){
					//GDB_MAKE_BREAKPOINT;
				}
				bitmap_unset(cc->map, i);
				last_ppa++;
			}
			else{
				bitmap_set(cc->map, i);
				last_ppa=ppa_list[i];
				new_head_array[new_head_idx++]=last_ppa;
			}
		}
		
	}
	if(new_head_idx<total_head){
		compftl_print_mapping(cc);
		printf("etr->idx:%u making error:%u\n",etr->idx, etr->physical_address);
		abort();
	}
	cc->head_array=new_head_array;
	return cc;
}

uint32_t compftl_insert_entry_from_translation(struct my_cache *, GTD_entry *etr, uint32_t /*lba*/, char *data, uint32_t *eviction_hint, uint32_t org_eviction_hint){
	if(etr->private_data){
		printf("already lru node exists! %s:%d\n", __FILE__, __LINE__);
		abort();
	}
/*
	bool test=false;
	if(etr->idx==711){
		static uint32_t cnt=0;
		if(cnt++==106){
			printf("break! %d\n",cnt++);
		}
	
		test=true;
	}*/
	compftl_cache *cc=make_cc_from_translation(etr, data);
	/*
	if(test){
		compftl_print_mapping(cc);
	}*/

	etr->private_data=(void *)lru_push(ccm.lru, (void*)cc);
	etr->status=CLEAN;
	ccm.now_caching_byte+=ccm.gtd_size[etr->idx];

	uint32_t target_size=ccm.gtd_size[etr->idx];
	(*eviction_hint)-=org_eviction_hint;
/*
	if(target_size+sizeof(uint32_t)*2!=org_eviction_hint){
		printf("changed_size\n");
		abort();
	}
*/
	compftl_size_checker(*eviction_hint);
	return 1;
}

uint32_t compftl_update_from_translation_gc(struct my_cache *, char *data, uint32_t lba, uint32_t ppa){
	uint32_t *ppa_list=(uint32_t*)data;
	uint32_t old_ppa=ppa_list[GETOFFSET(lba)];
	ppa_list[GETOFFSET(lba)]=ppa;
	return old_ppa;
}

void compftl_update_dynamic_size(struct my_cache *, uint32_t lba, char *data){
	uint32_t total_head=0;
	uint32_t last_ppa=0;
	bool sequential_flag=false;
	uint32_t *ppa_list=(uint32_t*)data;
	for(uint32_t i=0; i<PAGESIZE/sizeof(uint32_t); i++){
		if(i==0){
			last_ppa=ppa_list[i];
			total_head++;
		}
		else{
			sequential_flag=false;
			if(last_ppa+1==ppa_list[i]){
				sequential_flag=true;
			}

			if(sequential_flag){
				last_ppa++;
			}
			else{
				last_ppa=ppa_list[i];
				total_head++;
			}
		}
	}
	if(total_head<1 || total_head>PAGESIZE/sizeof(uint32_t)){
		printf("total_head over or small %s:%d\n", __FILE__, __LINE__);
		abort();
	}
	ccm.gtd_size[GETGTDIDX(lba)]=(total_head*sizeof(uint32_t)+BITMAPSIZE);
	//compftl_mapping_verify(cc);
}

uint32_t compftl_get_mapping(struct my_cache *, uint32_t lba){
	uint32_t gtd_idx=GETGTDIDX(lba);
	GTD_entry *etr=&dmm.GTD[gtd_idx];
	if(!etr->private_data){
		printf("insert data before pick mapping! %s:%d\n", __FILE__, __LINE__);
		abort();
	}

	return get_ppa_from_cc((compftl_cache*)((lru_node*)etr->private_data)->data, lba);
}

struct GTD_entry *compftl_get_eviction_GTD_entry(struct my_cache *, uint32_t lba){
	lru_node *target;
	GTD_entry *etr=NULL;
	for_each_lru_backword(ccm.lru, target){
		compftl_cache *cc=(compftl_cache*)target->data;
		etr=cc->etr;
		
		if(cc->etr->idx==0){
			//printf("start %u eviction \n", cc->etr->physical_address);
			//compftl_print_mapping(cc);
			//printf("end: %u eviction \n", cc->etr->physical_address);
		}
		if(etr->status==FLYING || etr->status==EVICTING){
			continue;
		}
		if(etr->status==CLEAN){
			etr->private_data=NULL;
			cc=(compftl_cache*)target->data;
			free(cc->head_array);
			bitmap_free(cc->map);
			lru_delete(ccm.lru, target);
			ccm.now_caching_byte-=ccm.gtd_size[etr->idx];
			return NULL;
		}

		if(etr->status!=DIRTY){
			printf("can't be status %s:%d\n", __FILE__, __LINE__);
			abort();
		}
		return etr;
	}
	return NULL;
}


bool compftl_update_eviction_target_translation(struct my_cache* ,uint32_t,  GTD_entry *etr,mapping_entry *map, char *data, void *, bool){
	compftl_cache *cc=(compftl_cache*)((lru_node*)etr->private_data)->data;

	bool target;
	uint32_t max=PAGESIZE/sizeof(uint32_t);
	uint32_t last_ppa=0;
	uint32_t head_idx=0;
	uint32_t *ppa_array=(uint32_t*)data;
	uint32_t ppa_array_idx=0;
	uint32_t offset=0;
	uint32_t total_head=0;
	/*
	if(etr->idx==535){
		total_head=(ccm.gtd_size[etr->idx]-BITMAPSIZE)/sizeof(uint32_t);
		compftl_mapping_verify(cc);
		printf("eviction start!\n");
		compftl_print_mapping(cc);
		printf("print done!\n");
	}*/

	for_each_bitmap_forward(cc->map, offset, target, max){	
		if(target){
			last_ppa=cc->head_array[head_idx++];
			ppa_array[ppa_array_idx++]=last_ppa;	
		}
		else{
			ppa_array[ppa_array_idx++]=++last_ppa;
		}
	}
/*
	if(etr->idx==535){
		compftl_cache *temp=make_cc_from_translation(etr, data);
		printf("eviction end!\n");
		free(temp->head_array);
		free(temp);
	}
*/
	free(cc->head_array);
	bitmap_free(cc->map);
	lru_delete(ccm.lru, (lru_node*)etr->private_data);
	etr->private_data=NULL;
	ccm.now_caching_byte-=ccm.gtd_size[etr->idx];
	return true;
}

bool compftl_exist(struct my_cache *, uint32_t lba){
	return dmm.GTD[GETGTDIDX(lba)].private_data!=NULL;
}

bool compftl_is_hit_eviction(struct my_cache *, GTD_entry *etr, uint32_t lba, uint32_t ppa, uint32_t total_hit_eviction){
	if(!etr->private_data) return false;
	compftl_cache *cc=GETSCFROMETR(etr);
	if(is_sequential(cc, lba, ppa)) return false;

	if(ccm.now_caching_byte+total_hit_eviction+sizeof(uint32_t)*2 > ccm.max_caching_byte){
		return true;
	}
	return false;
}

void compftl_force_put_mru(struct my_cache *, GTD_entry *etr,mapping_entry *map, uint32_t lba){
	lru_update(ccm.lru, (lru_node*)etr->private_data);
}

bool compftl_is_eviction_hint_full(struct my_cache *, uint32_t eviction_hint){
	return ccm.max_caching_byte <= eviction_hint;
}

uint32_t compftl_update_hit_eviction_hint(struct my_cache *, uint32_t lba, uint32_t *prefetching_info, uint32_t eviction_hint, 
		uint32_t *now_eviction_hint, bool increase){
	if(increase){
		*now_eviction_hint=sizeof(uint32_t)*2;
		return eviction_hint+*now_eviction_hint;
	}else{
		return eviction_hint-*now_eviction_hint;
	}
}

int32_t compftl_get_remain_space(struct my_cache *, uint32_t total_eviction_hint){
	return ccm.max_caching_byte-ccm.now_caching_byte-total_eviction_hint;
}

bool compftl_dump_cache_update(struct my_cache *, GTD_entry *etr, char *data){
	if(!etr->private_data) return false;
	compftl_cache *cc=(compftl_cache*)((lru_node*)etr->private_data)->data;

	bool target;
	uint32_t max=PAGESIZE/sizeof(uint32_t);
	uint32_t last_ppa=0;
	uint32_t head_idx=0;
	uint32_t *ppa_array=(uint32_t*)data;
	uint32_t ppa_array_idx=0;
	uint32_t offset=0;
	uint32_t total_head=0;
	

	for_each_bitmap_forward(cc->map, offset, target, max){	
		if(target){
			last_ppa=cc->head_array[head_idx++];
			ppa_array[ppa_array_idx++]=last_ppa;	
		}
		else{
			ppa_array[ppa_array_idx++]=++last_ppa;
		}
	}

	free(cc->head_array);
	bitmap_free(cc->map);
	lru_delete(ccm.lru, (lru_node*)etr->private_data);
	etr->private_data=NULL;
	ccm.now_caching_byte-=ccm.gtd_size[etr->idx];

	return true;
}


void compftl_load_specialized_meta(struct my_cache *cache, GTD_entry *etr, char *data){
	uint32_t *ppa_list=(uint32_t*)data;
	uint32_t head_num=1;
	if(etr->idx==4095){
		printf("break!\n");
	}
	for(uint32_t i=1; i<PAGESIZE/sizeof(uint32_t); i++){
		if(ppa_list[i]==ppa_list[i-1]+1){
		}
		else{
			head_num++;
		}
	}
	ccm.gtd_size[etr->idx]=head_num*sizeof(uint32_t)+BITMAPSIZE;
}

void compftl_empty_cache(struct my_cache *mc){
	uint32_t total_entry_num=0;
	while(1){
		compftl_cache *cc=(compftl_cache*)lru_pop(ccm.lru);
		if(!cc) break;
		uint32_t total_head=(ccm.gtd_size[cc->etr->idx]-BITMAPSIZE)/sizeof(uint32_t);
		total_entry_num+=PAGESIZE/sizeof(DMF);
		free(cc->head_array);
		bitmap_free(cc->map);
		cc->etr->private_data=NULL;
		free(cc);
	}

	uint32_t average_head_num=0;
	for(uint32_t i=0; i<GTDNUM; i++){
		uint32_t temp_head_num=(ccm.gtd_size[i]-BITMAPSIZE)/sizeof(uint32_t);
	//	pritnf("%u -> %u\n", i, temp_head_num);
		average_head_num+=temp_head_num;
	}
	printf("cached_entry_num:%u (%lf)\n", total_entry_num, (double)total_entry_num/RANGE);
	printf("average head num:%lf\n", (double)(average_head_num)/GTDNUM);

	ccm.now_caching_byte=0;
}
