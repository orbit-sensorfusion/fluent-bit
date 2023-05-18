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
        
        //printf("1 \n");
        //msgpack_object_print(stdout, msg_obj);
        //printf("2 \n");

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

                /* Value extracttion - value */
                p_toplevel_element++;
                double value = p_toplevel_element->val.via.f64;

                /* Set data in metrics context */
                bool found_metric = false;
                for (int i=0; i<ctx->cmts_size; i++) {
                    if (flb_sds_cmp(name, ctx->cmts[i].key, flb_sds_len(ctx->cmts[i].key)) == 0) {
                        flb_plg_trace(ctx->ins, "Setting metric for %s - %s - %s: %lu - %lu", labels[0], labels[1], name, ts, value);
                        cmt_gauge_set(ctx->cmts[i].value, ts, value, labels_size, labels);
                        found_metric = true;
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
    
    /* Append the updated metrics */
    ret = flb_input_metrics_append(ctx->ins, NULL, 0, ctx->cmt);
    if (ret != 0) {
        flb_plg_error(ctx->ins, "could not append metrics: %i", ret);
    }

    /* Cleanup */
    flb_free(pack);
    msgpack_unpacked_destroy(&result);
    flb_pack_state_reset(&ctx->state);
    flb_pack_state_init(&ctx->state);

    return ret;
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

    ctx->cmt = cmt_create();
    if (!ctx->cmt) {
        flb_plg_error(ctx->ins, "could not initialize CMetrics");
        flb_free(ctx);
        return NULL;
    }

    /* Init metrics */
    ctx->cmts_size = 19;
    ctx->cmts = (struct cmt_map*)flb_malloc(sizeof(struct cmt_map)*ctx->cmts_size);
    ctx->cmts[0].key = flb_sds_create("CloudUploadbytes");
    ctx->cmts[0].value = cmt_gauge_create(ctx->cmt,
                                "orbit", "sensor", "CloudUploadbytes",
                                "d",
                                3, (char *[]) {"serial_number", "sensor_mac", "index"});
    ctx->cmts[1].key = flb_sds_create("CloudDownloadbytes");
    ctx->cmts[1].value = cmt_gauge_create(ctx->cmt,
                                "orbit", "sensor", "CloudDownloadbytes",
                                "d",
                                3, (char *[]) {"serial_number", "sensor_mac", "index"}); 
    ctx->cmts[2].key = flb_sds_create("SupplyVoltage");
    ctx->cmts[2].value = cmt_gauge_create(ctx->cmt,
                                "orbit", "sensor", "SupplyVoltage",
                                "d",
                                3, (char *[]) {"serial_number", "sensor_mac", "index"});
    ctx->cmts[3].key = flb_sds_create("Accelerometer");
    ctx->cmts[3].value = cmt_gauge_create(ctx->cmt,
                                "orbit", "sensor", "Accelerometer",
                                "d",
                                3, (char *[]) {"serial_number", "sensor_mac", "index"});
    ctx->cmts[4].key = flb_sds_create("AmbientLightSensor");
    ctx->cmts[4].value = cmt_gauge_create(ctx->cmt,
                                "orbit", "sensor", "AmbientLightSensor",
                                "d",
                                3, (char *[]) {"serial_number", "sensor_mac", "index"});
    ctx->cmts[5].key = flb_sds_create("BVOCConcentrationSensor");
    ctx->cmts[5].value = cmt_gauge_create(ctx->cmt,
                                "orbit", "sensor", "BVOCConcentrationSensor",
                                "d",
                                3, (char *[]) {"serial_number", "sensor_mac", "index"});
    ctx->cmts[6].key = flb_sds_create("Barometer");
    ctx->cmts[6].value = cmt_gauge_create(ctx->cmt,
                                "orbit", "sensor", "Barometer",
                                "d",
                                3, (char *[]) {"serial_number", "sensor_mac", "index"});
    ctx->cmts[7].key = flb_sds_create("CO2ConcentrationSensor");
    ctx->cmts[7].value = cmt_gauge_create(ctx->cmt,
                                "orbit", "sensor", "CO2ConcentrationSensor",
                                "d",
                                3, (char *[]) {"serial_number", "sensor_mac", "index"});
    ctx->cmts[8].key = flb_sds_create("FuelGauge");
    ctx->cmts[8].value = cmt_gauge_create(ctx->cmt,
                                "orbit", "sensor", "FuelGauge",
                                "d",
                                3, (char *[]) {"serial_number", "sensor_mac", "index"});
    ctx->cmts[9].key = flb_sds_create("Gyroscope");
    ctx->cmts[9].value = cmt_gauge_create(ctx->cmt,
                                "orbit", "sensor", "Gyroscope",
                                "d",
                                3, (char *[]) {"serial_number", "sensor_mac", "index"});
    ctx->cmts[10].key = flb_sds_create("IAQSensor");
    ctx->cmts[10].value = cmt_gauge_create(ctx->cmt,
                                "orbit", "sensor", "IAQSensor",
                                "d",
                                3, (char *[]) {"serial_number", "sensor_mac", "index"});
    ctx->cmts[11].key = flb_sds_create("Magnetometer");
    ctx->cmts[11].value = cmt_gauge_create(ctx->cmt,
                                "orbit", "sensor", "Magnetometer",
                                "d",
                                3, (char *[]) {"serial_number", "sensor_mac", "index"});
    ctx->cmts[12].key = flb_sds_create("RelativeHumiditySensor");
    ctx->cmts[12].value = cmt_gauge_create(ctx->cmt,
                                "orbit", "sensor", "RelativeHumiditySensor",
                                "d",
                                3, (char *[]) {"serial_number", "sensor_mac", "index"});
    ctx->cmts[13].key = flb_sds_create("SupplyAmperage");
    ctx->cmts[13].value = cmt_gauge_create(ctx->cmt,
                                "orbit", "sensor", "SupplyAmperage",
                                "d",
                                3, (char *[]) {"serial_number", "sensor_mac", "index"});
    ctx->cmts[14].key = flb_sds_create("SupplyBatVoltage");
    ctx->cmts[14].value = cmt_gauge_create(ctx->cmt,
                                "orbit", "sensor", "SupplyBatVoltage",
                                "d",
                                3, (char *[]) {"serial_number", "sensor_mac", "index"});
    ctx->cmts[15].key = flb_sds_create("SupplyPower");
    ctx->cmts[15].value = cmt_gauge_create(ctx->cmt,
                                "orbit", "sensor", "SupplyPower",
                                "d",
                                3, (char *[]) {"serial_number", "sensor_mac", "index"});
    ctx->cmts[16].key = flb_sds_create("SupplySourceIndex");
    ctx->cmts[16].value = cmt_gauge_create(ctx->cmt,
                                "orbit", "sensor", "SupplySourceIndex",
                                "d",
                                3, (char *[]) {"serial_number", "sensor_mac", "index"});
    ctx->cmts[17].key = flb_sds_create("TVOCConcentrationSensor");
    ctx->cmts[17].value = cmt_gauge_create(ctx->cmt,
                                "orbit", "sensor", "TVOCConcentrationSensor",
                                "d",
                                3, (char *[]) {"serial_number", "sensor_mac", "index"});
    ctx->cmts[18].key = flb_sds_create("Thermometer");
    ctx->cmts[18].value = cmt_gauge_create(ctx->cmt,
                                "orbit", "sensor", "Thermometer",
                                "d",
                                3, (char *[]) {"serial_number", "sensor_mac", "index"});


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

    if (ctx->cmt) {
        flb_free(ctx->cmt);
    }

    s = &ctx->state;
    flb_pack_state_reset(s);
    flb_free(ctx);
    return 0;
}

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
    .event_type   = FLB_INPUT_METRICS
};