#include "./amf_info.h"
#include "./normal_write.h"

extern AmfManager *am;

extern void amf_error_call_back_r(void *req);
extern void amf_error_call_back_w(void *req);
extern void amf_error_call_back_e(void *req);

void normal_call_back_IO(void *req);
void normal_call_back_e(void *req);

pthread_cond_t wrapper_cond=PTHREAD_COND_INITIALIZER;
pthread_mutex_t wrapper_lock=PTHREAD_MUTEX_INITIALIZER;

amf_wrapper *wrapper_array;
std::queue<amf_wrapper*>* wrap_q;


static MeasureTime amf_time;

static inline amf_wrapper* get_amf_wrapper(uint32_t ppa, bool sync){
	amf_wrapper *res;
	pthread_mutex_lock(&wrapper_lock);
	while(wrap_q->empty()){
		pthread_cond_wait(&wrapper_cond, &wrapper_lock);
	}
	res=wrap_q->front();
	res->cnt=0;
	res->ppa=ppa;
	res->sync=sync;
	wrap_q->pop();
	pthread_mutex_unlock(&wrapper_lock);

	if(sync){
		fdriver_lock(&res->lock);
	}
	return res;
}

static inline void release_amf_wrapper(amf_wrapper *amw){
	pthread_mutex_lock(&wrapper_lock);
	if(amw->sync){
		fdriver_unlock(&amw->lock);
	}
	wrap_q->push(amw);
	pthread_cond_broadcast(&wrapper_cond);
	pthread_mutex_unlock(&wrapper_lock);
}

void normal_write_init(){
	SetReadCb(am,  normal_call_back_IO, amf_error_call_back_r);
	SetWriteCb(am,  normal_call_back_IO, amf_error_call_back_w);
	SetEraseCb(am, normal_call_back_e, amf_error_call_back_e);

	wrapper_array=(amf_wrapper*)malloc(sizeof(amf_wrapper)*QDEPTH);
	wrap_q=new std::queue<amf_wrapper*>();
	for(uint32_t i=0; i<QDEPTH; i++){
		fdriver_mutex_init(&wrapper_array[i].lock);
		wrap_q->push(&wrapper_array[i]);
	}
	measure_init(&amf_time);
}

static inline void __issue(uint32_t type, char *data, amf_wrapper *temp_req){
	for(uint32_t i=0; i<R2PGAP; i++){
		/*
		char temp[10]={0,};
		sprintf(temp, "%u",(temp_req->ppa*R2PGAP+i) & (AMF_PUNIT-1));
		measure_start(&amf_time);*/
		switch(type){
#ifndef TESTING
			case LOWER_WRITE:
				AmfWrite(am, temp_req->ppa*R2PGAP+i, &data[i*REAL_PAGE_SIZE], 
						(void *)temp_req);
				break;
			case LOWER_READ:
				AmfRead(am, temp_req->ppa*R2PGAP+i, &data[i*REAL_PAGE_SIZE], 
						(void *)temp_req);
				break;
			case LOWER_TRIM:
				AmfErase(am,temp_req->ppa*R2PGAP+i, NULL);
				break;

#else
			case LOWER_READ:
			case LOWER_WRITE:
				normal_call_back_IO((void*)temp_req);
				break;
			case LOWER_TRIM:
				break;
#endif
		}
		//measure_end(&amf_time, temp);
	}
}

void normal_write_issue(uint32_t type, uint32_t ppa, char *data,  algo_req *req){
	amf_wrapper *temp_req=NULL;
	if(type!=LOWER_TRIM){
		temp_req=get_amf_wrapper(ppa, false);
		temp_req->req=req;
	}
	__issue(type, data, temp_req);
}

void normal_write_sync_issue(uint32_t type, uint32_t ppa, char *data){
	amf_wrapper *temp_req=NULL;
	if(type!=LOWER_TRIM){
		temp_req=get_amf_wrapper(ppa, true);
		temp_req->req=NULL;
	}
	__issue(type, data, temp_req);
	fdriver_lock(&temp_req->lock);
	release_amf_wrapper(temp_req);
}

void normal_write_free(){
	free(wrapper_array);
	delete wrap_q;
}

void normal_call_back_IO(void *_req){
	amf_wrapper *wrapper=(amf_wrapper*)_req;

	wrapper->cnt++;
	if(wrapper->cnt==R2PGAP){
		if(wrapper->sync){
			fdriver_unlock(&wrapper->lock);
		}
		algo_req *req=(algo_req*)wrapper->req;
		req->end_req(req);
		if(!wrapper->sync){
			release_amf_wrapper(wrapper);
		}
	}
}

void normal_call_back_e(void *_req){

}

