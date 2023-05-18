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

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <fluent-bit/flb_info.h>
#include <fluent-bit/flb_input.h>
#include <fluent-bit/flb_input_plugin.h>
#include <fluent-bit/flb_config.h>
#include <fluent-bit/flb_utils.h>
#include <fluent-bit/flb_pack.h>
#include <fluent-bit/flb_error.h>
#include <fluent-bit/flb_metrics.h>
#include <fluent-bit/flb_metrics_exporter.h>
#include "in_lib_metrics.h"

#include <msgpack.h>

static int in_lib_collect(struct flb_input_instance *ins,
                          struct flb_config *config, void *in_context)
{
    int ret;
    int bytes;
    int pack_size;
    int capacity;
    int size;
    char *ptr;
    char *pack;
    struct flb_in_lib_config *ctx = in_context;

    capacity = (ctx->buf_size - ctx->buf_len);

    /* Allocate memory as required (FIXME: this will be limited in later) */
    if (capacity == 0) {
        size = ctx->buf_size + LIB_BUF_CHUNK;
        ptr = flb_realloc(ctx->buf_data, size);
        if (!ptr) {
            flb_errno();
            return -1;
        }
        ctx->buf_data = ptr;
        ctx->buf_size = size;
        capacity = LIB_BUF_CHUNK;
    }

    bytes = flb_pipe_r(ctx->fd,
                       ctx->buf_data + ctx->buf_len,
                       capacity);
    flb_plg_trace(ctx->ins, "in_lib read() = %i", bytes);
    if (bytes == -1) {
        perror("read");
        if (errno == -EPIPE) {
            return -1;
        }
        return 0;
    }
    ctx->buf_len += bytes;

    /* Transform from json string representation to message pack -> pack */
    ret = flb_pack_json_state(ctx->buf_data, ctx->buf_len,
                              &pack, &pack_size, &ctx->state);
    if (ret == FLB_ERR_JSON_PART) {
        flb_plg_warn(ctx->ins, "lib data incomplete, waiting for more data...");
        return 0;
    }
    else if (ret == FLB_ERR_JSON_INVAL) {
        flb_plg_warn(ctx->ins, "lib data invalid");
        flb_pack_state_reset(&ctx->state);
        flb_pack_state_init(&ctx->state);
        return -1;
    }
    ctx->buf_len = 0;

    msgpack_unpacked result;
    msgpack_object *msg_obj;
    size_t off = 0;    

    /* Iterate each item array and create metrics */
    msgpack_unpacked_init(&result);
    while (msgpack_unpack_next(&result, pack, pack_size, &off) == MSGPACK_UNPACK_SUCCESS) {

        msg_obj = &result.data;

        if (msg_obj->type == MSGPACK_OBJECT_MAP) {
            if (msg_obj->via.map.size == 4) {

                msgpack_object_kv *p_toplevel_element = msg_obj->via.map.ptr;

                /* Labels extraction - lables */
                size_t labels_size = p_toplevel_element->val.via.array.size;
                msgpack_object *label_p = p_toplevel_element->val.via.array.ptr;
                msgpack_object *const pend = p_toplevel_element->val.via.array.ptr + p_toplevel_element->val.via.array.size;
                flb_sds_t labels[labels_size];
                size_t i;
                for (i = 0; label_p < pend; ++label_p) {
                    msgpack_object label_obj = *label_p;
                    labels[i] = flb_sds_create_len(label_obj.via.str.ptr, label_obj.via.str.size);
                    i++;
                }

                /* Name extraction - name */
                p_toplevel_element++;
                flb_sds_t name = flb_sds_create_len(p_toplevel_element->val.via.str.ptr, p_toplevel_element->val.via.str.size);
                
                /* Timestamp extraction - ts */
                p_toplevel_element++;
                uint64_t ts;
                flb_sds_t ts_key = flb_sds_create_len(p_toplevel_element->key.via.str.ptr, p_toplevel_element->key.via.str.size);
                if (flb_sds_cmp(ts_key, "ts", flb_sds_len(ts_key)) == 0) {
                    flb_sds_destroy(ts_key);
                    ts = p_toplevel_element->val.via.i64;
                } else {
                    flb_plg_error(ctx->ins, "Cannot find timestamp");
                    return -1;
                }

                /* Value extraction - value */
                p_toplevel_element++;
                double value = p_toplevel_element->val.via.f64;

                /* Set data in metrics context */
                bool found_metric = false;
                for (int i = 0; i < ctx->gauges_size; i++) {
                    if (flb_sds_cmp(name, ctx->named_metrics_gauge[i].name, flb_sds_len(ctx->named_metrics_gauge[i].name)) == 0) {
                        found_metric = true;
                        
                        flb_plg_trace(ctx->ins, "Setting metric for %s - %s - %s: %llu - %.16f", labels[0], labels[1], name, ts, value);
                        cmt_gauge_set(ctx->named_metrics_gauge[i].gauge, ts, value, labels_size, labels);

                         /* Append the updated metrics */
                        ret = flb_input_metrics_append(ctx->ins, NULL, 0, ctx->named_metrics_gauge[i].cmt);
                        if (ret != 0) {
                            flb_plg_error(ctx->ins, "could not append metrics: %i", ret);
                        }
                    }
                }
                if (!found_metric) {
                    flb_plg_warn(ctx->ins, "No metric defined for %s", name);
                }

                // Clean-up
                flb_sds_destroy(name);
                for (i = 0; i < labels_size; ++i) {
                    flb_sds_destroy(labels[i]);
                }

            } else {
                flb_plg_error(ctx->ins, "Wrong number of elements");
                return -1;
            }
        } else {
            flb_plg_error(ctx->ins, "Wrong object type of message");
            return -1;
        }
    }

    /* Cleanup */
    flb_free(pack);
    msgpack_unpacked_destroy(&result);
    flb_pack_state_reset(&ctx->state);
    flb_pack_state_init(&ctx->state);

    return ret;
}

static int in_lib_configure(struct flb_in_lib_config *ctx) {
    struct mk_list *head = NULL;
    struct mk_list *split;
    struct mk_list *tmp;
    struct flb_split_entry *sentry;
    struct flb_config_map_val *gauge_definition_str;
    struct gauge_record *gauge_record;

    mk_list_init(&ctx->gauges);
    ctx->gauges_size = 0;

    /* Get labels (max 10) */
    // TODO: Can we remove the 10 as constraint?
    split = flb_utils_split(ctx->labels_str, ' ', 10);
    ctx->labels_size = mk_list_size(split);
    ctx->labels = flb_malloc(sizeof(char *) * (ctx->labels_size + 1));
    int i = 0;
    mk_list_foreach_safe(head, tmp, split) {
        sentry = mk_list_entry(head, struct flb_split_entry, _head);
        ctx->labels[i] = flb_strndup(sentry->value, sentry->len);
        i++;
    }

    /* Get gauge definitions */
    flb_config_map_foreach(head, gauge_definition_str, ctx->gauge_definitions) {
        gauge_record = flb_malloc(sizeof(struct gauge_record));
        if (!gauge_record) {
            flb_errno();
            return -1;
        }
        split = flb_utils_split(gauge_definition_str->val.str, ' ', 3);
        if (mk_list_size(split) != 4) {
            flb_plg_error(ctx->ins, "Invalid gauge parameter, expects 'NS SS NAME DESCRIPTION");
            flb_free(gauge_record);
            flb_utils_split_free(split);
            return -1;
        }

        /* Get Namespace (NS) */
        sentry = mk_list_entry_first(split, struct flb_split_entry, _head);
        gauge_record->ns = flb_strndup(sentry->value, sentry->len);

        /* Get Subsystem (SS) */
        sentry = mk_list_entry_next(&sentry->_head, struct flb_split_entry, _head, split);
        gauge_record->ss = flb_strndup(sentry->value, sentry->len);

        /* Get Name */
        sentry = mk_list_entry_next(&sentry->_head, struct flb_split_entry, _head, split);
        gauge_record->name = flb_strndup(sentry->value, sentry->len);

        /* Get Description */
        sentry = mk_list_entry_last(split, struct flb_split_entry, _head);
        gauge_record->description = flb_strndup(sentry->value, sentry->len);

        flb_utils_split_free(split);
        mk_list_add(&gauge_record->_head, &ctx->gauges);
        ctx->gauges_size++;
    }

    if (ctx->gauges_size <= 0) {
        flb_plg_error(ctx->ins, "at least one gauge is required");
        return -1;
    }

    /* Add gauge definitions */
    ctx->named_metrics_gauge = (struct named_metric_gauge*)flb_malloc(sizeof(struct named_metric_gauge)*ctx->gauges_size);
    i = 0;
    mk_list_foreach_safe(head, tmp, &ctx->gauges) {
        gauge_record = mk_list_entry(head, struct gauge_record, _head);
        flb_plg_trace(ctx->ins, "Create gauge with name %s", gauge_record->name) ;
        ctx->named_metrics_gauge[i].name = flb_sds_create(gauge_record->name);
        ctx->named_metrics_gauge[i].cmt = cmt_create();

        if (!ctx->named_metrics_gauge[i].cmt) {
            flb_plg_error(ctx->ins, "Could not initialize CMetrics");
            flb_free(ctx);
            return -1;
        }

        ctx->named_metrics_gauge[i].gauge = cmt_gauge_create(ctx->named_metrics_gauge[i].cmt,
                                                                gauge_record->ns, gauge_record->ss, gauge_record->name,
                                                                gauge_record->description, ctx->labels_size, ctx->labels);
        i++;
    }
    return 0;
}

/* Initialize plugin */
static int in_lib_init(struct flb_input_instance *in,
                       struct flb_config *config, void *data)
{
    int ret;
    struct flb_in_lib_config *ctx;
    (void) data;

    /* Allocate space for the configuration */
    ctx = flb_malloc(sizeof(struct flb_in_lib_config));
    if (!ctx) {
        return -1;
    }
    ctx->ins = in;

    /* Buffer for incoming data */
    ctx->buf_size = LIB_BUF_CHUNK;
    ctx->buf_data = flb_calloc(1, LIB_BUF_CHUNK);
    ctx->buf_len = 0;

    if (!ctx->buf_data) {
        flb_errno();
        flb_plg_error(ctx->ins, "Could not allocate initial buf memory buffer");
        flb_free(ctx);
        return -1;
    }

    /* Load the config map */
    ret = flb_input_config_map_set(in, (void *)ctx);
    if (ret == -1) {
        flb_plg_error(in, "unable to load configuration");
        flb_free(ctx);
        return -1;
    }

    /* Configure labels and CMetrics contexts */
    if (in_lib_configure(ctx) < 0) {
        return -1;
    }

    /* Init communication channel */
    flb_input_channel_init(in);
    ctx->fd = in->channel[0];

    /* Set the context */
    flb_input_set_context(in, ctx);

    /* Collect upon data available on the standard input */
    ret = flb_input_set_collector_event(in,
                                        in_lib_collect,
                                        ctx->fd,
                                        config);
    if (ret == -1) {
        flb_plg_error(ctx->ins, "Could not set collector for LIB input plugin");
        flb_free(ctx->buf_data);
        flb_free(ctx);
        return -1;
    }

    flb_pack_state_init(&ctx->state);
    return 0;
}

static int in_lib_exit(void *data, struct flb_config *config)
{
    (void) config;
    struct flb_in_lib_config *ctx = data;
    struct flb_pack_state *s;

    if (ctx->buf_data) {
        flb_free(ctx->buf_data);
    }

    // TODO: Check if we have to free more (e.g. cmt within named_metrics_gauge)
    if (ctx->named_metrics_gauge) {
        flb_free(ctx->named_metrics_gauge);
    }

    s = &ctx->state;
    flb_pack_state_reset(s);
    flb_free(ctx);
    return 0;
}

/* Configuration properties map */
static struct flb_config_map config_map[] = {
    {
     FLB_CONFIG_MAP_STR, "labels", "default",
     0, FLB_TRUE, offsetof(struct flb_in_lib_config, labels_str),
     "Add labels to your metrics"
    },
    {
     FLB_CONFIG_MAP_STR, "gauge", NULL,
     FLB_CONFIG_MAP_MULT, FLB_TRUE, offsetof(struct flb_in_lib_config, gauge_definitions),
     "Add gauge metrics"
    },

    /* EOF */
    {0}
};

/* Plugin reference */
struct flb_input_plugin in_lib_metrics_plugin = {
    .name         = "lib_metrics",
    .description  = "Library mode Input for metrics",
    .cb_init      = in_lib_init,
    .cb_pre_run   = NULL,
    .cb_collect   = NULL,
    .cb_ingest    = NULL,
    .cb_flush_buf = NULL,
    .cb_exit      = in_lib_exit,
    .config_map   = config_map,
    .event_type   = FLB_INPUT_METRICS
};