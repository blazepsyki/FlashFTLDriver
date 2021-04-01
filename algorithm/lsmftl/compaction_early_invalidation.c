#include "compaction.h"
extern lsmtree LSM;

extern compaction_master *_cm;
extern void read_map_param_init(read_issue_arg *read_arg, map_range *mr);
extern bool read_map_done_check(inter_read_alreq_param *param, bool check_page_sst);
static bool early_map_done(inter_read_alreq_param *param){
	param->map_target->data=NULL;
	inf_free_valueset(param->data, FS_MALLOC_R);
	fdriver_destroy(&param->done_lock);
	free(param);
	return true;
}

static uint32_t stream_invalidation(sst_pf_out_stream *os, uint32_t ridx){
	uint32_t res=0;
	uint32_t a, b;
	while(1){
		key_ptr_pair kp=sst_pos_pick(os);
		if(kp.lba!=UINT32_MAX){
			a=version_map_lba(LSM.last_run_version, kp.lba);
			b=ridx;
			if(version_compare(LSM.last_run_version, a, b) > 0){
				invalidate_piece_ppa(LSM.pm->bm, kp.piece_ppa, true);
				res++;
			}
		}
		sst_pos_pop(os);
		if(sst_pos_is_empty(os)){
			break;
		}
	}
	return res;
}


static map_range *get_run_maprange(run *r, uint32_t *total_num){
	sst_file *sptr;
	map_range *mptr;
	uint32_t sidx, midx;
	uint32_t map_total_num=0;
	for_each_sst(r, sptr, sidx){
		map_total_num+=sptr->map_num;
	}

	map_range *res=(map_range*)malloc(sizeof(map_range)*map_total_num);
	uint32_t now_midx=0;
	for_each_sst(r, sptr, sidx){
		for_each_map_range(sptr, mptr, midx){
			res[now_midx++]=*mptr;
		}
	}

	(*total_num)=map_total_num;
	return res;
}

uint32_t compaction_early_invalidation(uint32_t input_ridx){
	uint32_t total_invalidation_cnt=0;
	_cm=LSM.cm;
	/*check gc unavailable*/
	uint32_t target_invalidation_cnt=0;
	uint32_t target_ridx=input_ridx;
	version *v=LSM.last_run_version;
	if(target_ridx==UINT32_MAX){
		target_ridx=version_get_max_invalidation_target(v, &target_invalidation_cnt, NULL);
	}

	if(target_ridx==UINT32_MAX) return 0;

	LSM.monitor.compaction_early_invalidation_cnt++;

	printf("early_invalidation:%u target_ridx:%u\n", LSM.monitor.compaction_early_invalidation_cnt, target_ridx);
	read_issue_arg read_arg;
	read_arg_container thread_arg;
	thread_arg.end_req=comp_alreq_end_req;
	thread_arg.arg_set=(read_issue_arg**)malloc(sizeof(read_issue_arg));
	thread_arg.arg_set[0]=&read_arg;
	thread_arg.set_num=1;

	sst_pf_out_stream *pos;

	run *target_r=&LSM.disk[LSM.param.LEVELN-1]->array[target_ridx];

	uint32_t start_idx=0;
	uint32_t total_num=0;
	map_range *target_mr_set=get_run_maprange(target_r, &total_num);

	uint32_t compaction_tag_num=compaction_read_param_remain_num(_cm);
	uint32_t round=(total_num/compaction_tag_num)+(total_num%compaction_tag_num?1:0);


	for(uint32_t i=0; i<round; i++){
		read_arg.from=start_idx+i*compaction_tag_num;
		if(i!=round-1){
			read_arg.to=start_idx+(i-1)*compaction_tag_num-1;
		}
		else{
			read_arg.to=total_num-1;
		}	
		read_map_param_init(&read_arg, target_mr_set);

		if(i==0){
			pos=sst_pos_init_mr(&target_mr_set[read_arg.from], read_arg.param, 
					TARGETREADNUM(read_arg), read_map_done_check, early_map_done);
		}
		else{
			sst_pos_add_mr(pos, &target_mr_set[read_arg.from], read_arg.param, 
					TARGETREADNUM(read_arg));
		}

		thpool_add_work(_cm->issue_worker, read_sst_job, (void*)&thread_arg);
		total_invalidation_cnt+=stream_invalidation(pos, target_ridx);
	}

	sst_pos_free(pos);
	free(thread_arg.arg_set);
	free(target_mr_set);
	version_set_early_invalidation(LSM.last_run_version, target_ridx);
	return total_invalidation_cnt;
}