/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  Fluent Bit
 *  ==========
 *  Copyright (C) 2015-2022 The Fluent Bit Authors
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

#ifndef FLB_MQTT_H
#define FLB_MQTT_H

#include <fluent-bit/flb_output_plugin.h>
#include <fluent-bit/flb_sds.h>

#include <mosquitto.h>

struct flb_out_mqtt {
    /* Public configuration properties */
    flb_sds_t broker_host;
    int broker_port;
    flb_sds_t topic;
    flb_sds_t payload;
    flb_sds_t client_id;
    int qos;
    flb_sds_t protocol_version;
    int broker_tls;
    flb_sds_t broker_tls_ca_file;
    flb_sds_t broker_tls_cert_file;
    flb_sds_t broker_tls_key_file;

    /* Internal variables to store the placeholders during init */
    int num_placeholders_topic;
    flb_sds_t* placeholder_names_topic;
    int num_placeholders_payload;
    flb_sds_t* placeholder_names_payload;

    /* MQTT Client instance */
    struct mosquitto *mosq;

    /* Plugin instance */
    struct flb_output_instance *ins;
};

#endif
