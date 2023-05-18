/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  Fluent Bit
 *  ==========
 *  Copyright (C) 2019-2021 The Fluent Bit Authors
 *  Copyright (C) 2015-2018 Treasure Data Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#ifndef FLB_IN_LIB_METRICS_H
#define FLB_IN_LIB_METRICS_H

#include <fluent-bit/flb_config.h>
#include <fluent-bit/flb_input.h>
#include <fluent-bit/flb_pack.h>
#include <fluent-bit/flb_input_plugin.h>

#define LIB_BUF_CHUNK   65536

struct named_metric_gauge {
    flb_sds_t name;             /* Metrics Name     */
    struct cmt *cmt;            /* CMetrics Context */
    struct cmt_gauge *gauge;    /* CMetrics Gauge   */
};

struct gauge_record {
    char *ns;
    char *ss;
    char *name;
    char *description;
    struct mk_list _head;
};

/* Library input configuration & context */
struct flb_in_lib_config {

    int fd;                     /* instance input channel  */
    int buf_size;               /* buffer size / capacity  */
    int buf_len;                /* read buffer length      */
    char *buf_data;             /* the real buffer         */

    struct flb_pack_state state;
    struct flb_input_instance *ins;
    
    /* config options */
    // Labels
    flb_sds_t labels_str; 
    int labels_size;
    char **labels;
    // Gauge Definitions
    struct mk_list *gauge_definitions;
    struct mk_list gauges;
    int gauges_size;

    /* An independent CMetrics metric for each gauge metric to record */
    struct named_metric_gauge *named_metrics_gauge;   
};

#endif