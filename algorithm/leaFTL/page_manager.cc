#include "./page_manager.h"
#include "./issue_io.h"
#include "../../interface/interface.h"
#include "../../include/data_struct/list.h"
#include <string.h>
#include <algorithm>
typedef struct io_param{
    uint32_t type;
    uint32_t ppa;
    bool isdone;
    value_set *value;
}io_param;


typedef struct gc_node{
    uint32_t lba;
    char *value;
}gc_node;

bool gc_node_cmp(gc_node a, gc_node b){
    return a.lba < b.lba;
}

void g_buffer_init(align_buffer *g_buffer){
    g_buffer->value=(char**)malloc(sizeof(char*)*L2PGAP);
    for(uint32_t i=0; i<L2PGAP; i++){
        g_buffer->value[i]=(char*)malloc(LPAGESIZE);
    }
    g_buffer->idx=0;
}

void g_buffer_free(align_buffer *g_buffer){
    for(uint32_t i=0; i<L2PGAP; i++){
        free(g_buffer->value[i]);
    }
    free(g_buffer->value);
}

void g_buffer_to_temp_map(align_buffer *g_buffer, temp_map *res, uint32_t *piece_ppa_arr){
    for(uint32_t idx=0; idx<g_buffer->idx; idx++){
        res->lba[res->size]=g_buffer->key[idx];
        res->piece_ppa[res->size]=piece_ppa_arr[idx];
        res->size++;
    }
    g_buffer->idx=0;
}

void g_buffer_insert(align_buffer *g_buffer, char *data, uint32_t lba){
    memcpy(g_buffer->value[g_buffer->idx], data, LPAGESIZE);
    g_buffer->key[g_buffer->idx]=lba;
    g_buffer->idx++;
}

static void invalidate_piece_ppa(blockmanager *bm, uint32_t piece_ppa){
    bm->bit_unset(bm, piece_ppa);
}

static void validate_ppa(blockmanager *bm, uint32_t ppa, KEYT *lba, uint32_t max_idx){
    for(uint32_t i=0; i<max_idx; i++){
        bm->bit_set(bm, ppa*L2PGAP+i);
    }
    bm->set_oob(bm, (char*)lba, sizeof(uint32_t) * max_idx, ppa);
}

page_manager *pm_init(lower_info *li, blockmanager *bm){
    page_manager *res=(page_manager*)malloc(sizeof(page_manager));
    res->lower=li;
    res->bm=bm;

    res->reserve=res->bm->get_segment(res->bm, BLOCK_RESERVE);
    res->active=res->bm->get_segment(res->bm, BLOCK_ACTIVE);
    res->usedup_segments=new std::set<uint32_t>();
    return res;
}

bool pm_assign_new_seg(page_manager *pm){
    blockmanager *bm = pm->bm;
    std::pair<std::set<uint32_t>::iterator, bool> temp = pm->usedup_segments->insert(pm->active->seg_idx);
    if (temp.second == false){ // insert fail
        printf("error already existing segment in the usedup set!, seg_idx:%u\n", pm->active->seg_idx);
        abort();
    }
    if(bm->is_gc_needed(bm)){
        return false;
    }
    
    pm->active = bm->get_segment(bm, BLOCK_ACTIVE);

    if (pm_remain_space(pm, true) == 0){
        printf("error on getting new active block, already full!\n");
        abort();
    }
    return true;
}

static inline ppa_t get_ppa(page_manager *pm, blockmanager *bm, __segment *seg){
    if(bm->check_full(seg)){
        printf("already full segment %s:%u\n", __FUNCTION__, __LINE__);
        abort();
        return UINT32_MAX;
    }
    ppa_t res=bm->get_page_addr(seg);
    return res;
}

void *pm_end_req(algo_req * const al_req){
    io_param *params=(io_param*)al_req->param;
    switch(params->type){
        case GCDW:
        case DATAW:
            inf_free_valueset(params->value, FS_MALLOC_W);
            free(params);
            break;
        case GCDR:
            params->isdone=true;
            break;
    }
    free(al_req);
    return NULL;
}

static inline io_param *make_io_param(uint32_t type, uint32_t ppa, value_set *value){
    io_param *res=(io_param*)malloc(sizeof(io_param));
    res->type=type;
    res->ppa=ppa;
    res->isdone=false;
    res->value=value;
    return res;
}

void pm_page_flush(page_manager *pm, bool isactive, uint32_t type, uint32_t *lba, char **data, uint32_t size, uint32_t *piece_ppa_res){
    ppa_t ppa=get_ppa(pm, pm->bm, isactive? pm->active: pm->reserve);
    if(ppa==UINT32_MAX){
        printf("you should call gc before flushing data!\n");
        abort();
    }
    
    value_set *value=inf_get_valueset(NULL, FS_MALLOC_W, PAGESIZE);
    for(uint32_t i=0; i<size; i++){
        memcpy(&value->value[i*LPAGESIZE], data[i], LPAGESIZE);
        piece_ppa_res[i]=ppa*L2PGAP+i;
    }
    io_param *params=make_io_param(DATAW, ppa, value);

    send_IO_back_req(type, pm->lower, ppa, value, (void*)params, pm_end_req);
    validate_ppa(pm->bm, ppa, lba, size);
}

uint32_t pm_remain_space(page_manager *pm, bool isactive){
    if(pm->bm->check_full(isactive?pm->active:pm->reserve)){
        return 0;
    }
    else{
        return _PPS-(pm->bm->pick_page_addr(isactive?pm->active:pm->reserve)%_PPS);
    }
}

__gsegment* pm_get_gc_target(blockmanager *bm){
    return bm->get_gc_target(bm);
}

static io_param *send_gc_req(lower_info *lower, uint32_t ppa, uint32_t type, value_set *value){
    io_param *res=NULL;
    value_set *target_value;
    switch(type){
        case GCMR:
        printf("not implemented! %s:%d\n", __FUNCTION__, __LINE__);
        abort();
        break;
        case GCDR:
        target_value=inf_get_valueset(NULL,FS_MALLOC_R, PAGESIZE);
        res=make_io_param(type, ppa, target_value);
        break;
        case GCDW:
        target_value=value;
        break;
    }
    send_IO_back_req(type, lower, ppa, target_value, res, pm_end_req);
    return res;
} 

void pm_gc(page_manager *pm, __gsegment *target, temp_map *res, bool isdata, bool (*ignore)(uint32_t lba)){
    uint32_t page;
    uint32_t bidx, pidx;
    blockmanager* bm=pm->bm;
    list *temp_list=list_init();
    io_param *gp; //gc_param;

    /*read phase*/
    for_each_page_in_seg(target, page, bidx, pidx){
        bool should_read=false;
        for(uint32_t i=0; i<L2PGAP; i++){
            if(bm->is_invalid_piece(bm, page*L2PGAP+i)) continue;
            else{
                should_read=true;
                break;
            }
        }
        if(should_read){
            gp=send_gc_req(pm->lower, page, isdata? GCDR: GCMR, NULL);
            list_insert(temp_list, (void*)gp);
        }
    }

    
    /*converting read data*/
    li_node *now, *nxt;
    uint32_t* lba_arr;
    std::vector<gc_node> temp_vector;
    gc_node gn;
    while(temp_list->size){
        for_each_list_node_safe(temp_list, now, nxt){
            gp=(io_param*)now->data;
            if(gp->isdone==false) continue;
            lba_arr=(uint32_t*)bm->get_oob(bm, gp->ppa);
            for(uint32_t i=0; i<L2PGAP; i++){
                if(bm->is_invalid_piece(bm, gp->ppa*L2PGAP+i)) continue;
                if(ignore && ignore(lba_arr[i])) continue;

                gn.lba=lba_arr[i];
                gn.value=&gp->value->value[i*LPAGESIZE];
                temp_vector.push_back(gn);
            }
        }
    }

    if(isdata){ /*preprocessing*/
        sort(temp_vector.begin(), temp_vector.end(), gc_node_cmp);
    }


    /*write_data*/
    align_buffer g_buffer;
    g_buffer_init(&g_buffer);
    uint32_t piece_ppa_arr[L2PGAP];
    for(uint32_t i=0; i<temp_vector.size(); i++){
        g_buffer_insert(&g_buffer, temp_vector[i].value, temp_vector[i].lba);
        if (g_buffer.idx == L2PGAP || (i==temp_vector.size()-1 && g_buffer.idx!=0)){
            pm_page_flush(pm, false, GCDW, g_buffer.key, g_buffer.value, g_buffer.idx, piece_ppa_arr);
            g_buffer_to_temp_map(&g_buffer, res, piece_ppa_arr);
        }
    }
    g_buffer_free(&g_buffer);

    /*clean memory*/
    for_each_list_node_safe(temp_list, now, nxt){
        gp=(io_param*)now->data;
        inf_free_valueset(gp->value, FS_MALLOC_R);
        free(gp);
        list_delete_node(temp_list, now);
    }
    list_free(temp_list);

    /*reset active segment and reserve segment*/   
    bm->trim_segment(bm ,target);
    pm->active=pm->reserve;
    bm->change_reserve_to_active(bm, pm->reserve);
    pm->reserve=bm->get_segment(bm, BLOCK_RESERVE);
}

void pm_free(page_manager *pm){
    free(pm);
}