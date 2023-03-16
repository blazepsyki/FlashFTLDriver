#pragma once
#include "../../include/settings.h"
#include "../../include/container.h"
#include "../../include/debug_utils.h"
#include "./group.h"

#define COMPACTION_RESOLUTION 1000000

uint32_t lea_create(lower_info *, blockmanager *, algorithm *);
void lea_destroy(lower_info *, algorithm *);
uint32_t lea_argument(int argc, char **argv);
uint32_t lea_read(request *const);
uint32_t lea_write(request *const);
uint32_t lea_remove(request *const);

uint32_t *lea_gp_to_mapping(group *gp);
void lea_cache_insert(group *gp, uint32_t *piece_ppa);
bool lea_cache_evict(group *gp);
void lea_cache_promote(group *gp);