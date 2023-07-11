#include <fluent-bit/flb_output_plugin.h>
#include <fluent-bit/flb_utils.h>
#include <fluent-bit/flb_slist.h>
#include <fluent-bit/flb_time.h>
#include <fluent-bit/flb_pack.h>
#include <fluent-bit/flb_config_map.h>
#include <fluent-bit/flb_metrics.h>

#include <msgpack.h>

#include "mqtt.h"

// TODO: Error Handling for FB
static char* repl_str(const char *str, const char *from, const char *to) {
    /* Original from https://creativeandcritical.net/str-replace-c */

	/* Increment positions cache size initially by this number. */
	size_t cache_sz_inc = 16;
	/* Thereafter, each time capacity needs to be increased,
	 * multiply the increment by this factor. */
	const size_t cache_sz_inc_factor = 3;
	/* But never increment capacity by more than this number. */
	const size_t cache_sz_inc_max = 1048576;

	char *pret, *ret = NULL;
	const char *pstr2, *pstr = str;
	size_t i, count = 0;
	uintptr_t *pos_cache_tmp, *pos_cache = NULL;
	size_t cache_sz = 0;
	size_t cpylen, orglen, retlen, tolen, fromlen = strlen(from);

	/* Find all matches and cache their positions. */
	while ((pstr2 = strstr(pstr, from)) != NULL) {
		count++;

		/* Increase the cache size when necessary. */
		if (cache_sz < count) {
			cache_sz += cache_sz_inc;
			pos_cache_tmp = realloc(pos_cache, sizeof(*pos_cache) * cache_sz);
			if (pos_cache_tmp == NULL) {
				goto end_repl_str;
			} else pos_cache = pos_cache_tmp;
			cache_sz_inc *= cache_sz_inc_factor;
			if (cache_sz_inc > cache_sz_inc_max) {
				cache_sz_inc = cache_sz_inc_max;
			}
		}

		pos_cache[count-1] = pstr2 - str;
		pstr = pstr2 + fromlen;
	}

	orglen = pstr - str + strlen(pstr);

	/* Allocate memory for the post-replacement string. */
	if (count > 0) {
		tolen = strlen(to);
		retlen = orglen + (tolen - fromlen) * count;
	} else	retlen = orglen;
	ret = malloc(retlen + 1);
	if (ret == NULL) {
		goto end_repl_str;
	}

	if (count == 0) {
		/* If no matches, then just duplicate the string. */
		strcpy(ret, str);
	} else {
		/* Otherwise, duplicate the string whilst performing
		 * the replacements using the position cache. */
		pret = ret;
		memcpy(pret, str, pos_cache[0]);
		pret += pos_cache[0];
		for (i = 0; i < count; i++) {
			memcpy(pret, to, tolen);
			pret += tolen;
			pstr = str + pos_cache[i] + fromlen;
			cpylen = (i == count-1 ? orglen : pos_cache[i+1]) - pos_cache[i] - fromlen;
			memcpy(pret, pstr, cpylen);
			pret += cpylen;
		}
		ret[retlen] = '\0';
	}

end_repl_str:
	/* Free the cache and return the post-replacement string,
	 * which will be NULL in the event of an error. */
	free(pos_cache);
	return ret;
}

static int extract_placeholder_names(struct flb_out_mqtt *ctx, flb_sds_t string, int* num_placeholders_ptr, flb_sds_t** placeholder_names_ptr) {
    flb_sds_t* placeholder_names_tmp = NULL;
    flb_sds_t* placeholder_names = NULL;
    int placeholder_count = 0;

    // Find all occurrences of "$(<placeholder_name>)" within the string
    flb_sds_t start = string;
    while ((start = strstr(start, "$(")) != NULL) {
        flb_sds_t end = strstr(start + 2, ")");
        if (end == NULL) {
            flb_plg_error(ctx->ins, "Error while extracting placeholder names from string: %s.", string);
            return -1;
            break;
        }

        // Extract the placeholder name
        int name_length = end - start - 2;
        flb_sds_t name = (flb_sds_t)flb_malloc(name_length + 1);
        if (name == NULL) {
            flb_errno();
            free(placeholder_names);
            return -1;
        }
        strncpy(name, start + 2, name_length);
        name[name_length] = '\0';

        // Add the placeholder name to the array
        placeholder_count++;
        placeholder_names_tmp = (flb_sds_t*)flb_realloc(placeholder_names, placeholder_count * sizeof(flb_sds_t));
        if (placeholder_names_tmp == NULL) {
            flb_errno();
            free(name);
            free(placeholder_names);
            return -1;
        } else {
            placeholder_names = placeholder_names_tmp;
        }
        placeholder_names[placeholder_count - 1] = name;

        start = end + 1;
    }

    // Set the number of placeholders and the array of placeholder names
    *num_placeholders_ptr = placeholder_count;
    *placeholder_names_ptr = placeholder_names;
    return 0;
}

static int get_value_for_json_pointer(struct flb_out_mqtt *ctx, msgpack_object root, const flb_sds_t json_pointer, flb_sds_t *json_value, bool quoted_str) {
    // Remark: Arrays are not supported in JSON pointer at the moment

    char* json_pointer_token = flb_malloc(strlen(json_pointer) + 1);
    if (json_pointer_token == NULL) {
        flb_errno();
        return FLB_ERROR;
    }
    strcpy(json_pointer_token, json_pointer);

    msgpack_object current_obj = root;
    char* token = strtok(json_pointer_token, "/");
    while (token != NULL) {
        if (current_obj.type != MSGPACK_OBJECT_MAP) {
            flb_plg_error(ctx->ins, "Error while getting value for JSON pointer: Object is not a map.");
            flb_free(json_pointer_token);
            return FLB_ERROR;
        }

        msgpack_object_kv* kvs = current_obj.via.map.ptr;
        msgpack_object_kv* end = kvs + current_obj.via.map.size;
        msgpack_object_kv* kv = NULL;
        for (kv = kvs; kv != end; kv++) {
            if (kv->key.type != MSGPACK_OBJECT_STR) {
                flb_plg_error(ctx->ins, "Error while getting value for JSON pointer: Map key is not a string.");
                flb_free(json_pointer_token);
                return FLB_ERROR;
            }
            
            if ((strlen(token) == (int) kv->key.via.str.size) && (strncmp(kv->key.via.str.ptr, token, kv->key.via.str.size) == 0)) {
                current_obj = kv->val;
                break;
            }
        }

        if (kv == end) {
            flb_plg_error(ctx->ins, "Error while getting value for JSON pointer: Key '%s' not found (JSON pointer: %s).", token, json_pointer);
            flb_free(json_pointer_token);
            return FLB_ERROR;
        }

        token = strtok(NULL, "/");
    }
    
    /* Get the value and convert it to a string. We need a special
     * handling for strings depending if they need to be quoted
     * (e.g. for the payload) or not (e.g. for the topic).
     */
    flb_sds_destroy(*json_value);
    switch(current_obj.type) {
        case MSGPACK_OBJECT_STR:
            if (quoted_str) {
                // We requested to extract string values with quotes
                char* str_json = flb_msgpack_to_json_str(0, &current_obj);
                *json_value = flb_sds_create(str_json);
                flb_free(str_json);
            } else {
                flb_sds_t msgpack_value = flb_sds_create_len(current_obj.via.str.ptr, (int) current_obj.via.str.size);
                *json_value = flb_sds_create(msgpack_value);
                flb_sds_destroy(msgpack_value);
            }
            break;
        default:
            ; // Empty statement
            char* msgpack_json = flb_msgpack_to_json_str(0, &current_obj);
            *json_value = flb_sds_create(msgpack_json);
            flb_free(msgpack_json);
    }

    flb_free(json_pointer_token);
    return FLB_OK;
}

static int replace_placeholder_with_value(flb_sds_t *output, flb_sds_t input, flb_sds_t placeholder_name, flb_sds_t placeholder_value) {
    const flb_sds_t placeholder_start = flb_sds_create("$(");
    const flb_sds_t placeholder_end = flb_sds_create(")");
    flb_sds_t placeholder_full = flb_sds_create(placeholder_start);
    placeholder_full = flb_sds_cat(placeholder_full, placeholder_name, strlen(placeholder_name));
    placeholder_full = flb_sds_cat(placeholder_full, placeholder_end, 1);

    char* result = repl_str(input, placeholder_full, placeholder_value);

    flb_sds_destroy(placeholder_start);
    flb_sds_destroy(placeholder_end);
    flb_sds_destroy(placeholder_full);

    flb_sds_destroy(*output);
    *output = flb_sds_create(result);
    flb_free(result);

    return FLB_OK;
}

static int replace_all_placeholders(struct flb_out_mqtt *ctx, msgpack_object root, flb_sds_t input, flb_sds_t *output, flb_sds_t *placeholder_names, int num_placeholders, bool quoted_str) {
    flb_sds_t result = flb_sds_create_size(0);
    flb_sds_t result_tmp = input;
    for (int i = 0; i < num_placeholders; i++) {
        const flb_sds_t placeholder_name = placeholder_names[i];
        flb_sds_t placeholder_value = flb_sds_create_size(0);
        int ret = get_value_for_json_pointer(ctx, root, placeholder_name, &placeholder_value, quoted_str);
        if (ret == FLB_ERROR) {
            flb_sds_destroy(placeholder_value);
            return FLB_ERROR;
        }

        ret = replace_placeholder_with_value(&result, result_tmp, placeholder_name, placeholder_value);
        result_tmp = result;
        flb_sds_destroy(placeholder_value);
        if (ret == FLB_ERROR) {
            return FLB_ERROR;
        }
    }
    
    flb_sds_destroy(*output);
    *output = result;
    
    return FLB_OK;
}

static int cb_mqtt_init(struct flb_output_instance *ins, struct flb_config *config, void *data)
{
    int ret;
    struct flb_out_mqtt *ctx = NULL;
    (void) ins;
    (void) config;
    (void) data;

    ctx = flb_calloc(1, sizeof(struct flb_out_mqtt));
    if (!ctx) {
        flb_errno();
        return -1;
    }
    ctx->ins = ins;

    /* Read in config values */
    ret = flb_output_config_map_set(ins, (void *) ctx);
    if (ret == -1) {
        flb_free(ctx);
        return -1;
    }

    // TODO: We should validate the entire config (e.g. is the topic valid, i.e. no spaces etc., ...)

    /* Extract all placeholders for topic and payload */
    ret = extract_placeholder_names(ctx, ctx->topic, &(ctx->num_placeholders_topic), &(ctx->placeholder_names_topic));
    if (ret == -1) {
        flb_free(ctx);
        return -1;
    }
    ret = extract_placeholder_names(ctx, ctx->payload, &(ctx->num_placeholders_payload), &(ctx->placeholder_names_payload));
    if (ret == -1) {
        flb_free(ctx);
        return -1;
    }

    /* Create MQTT Client instance */
    mosquitto_lib_init();

    // TODO: What is the correct client-id? Do we need to include the process ID?
    ctx->mosq = mosquitto_new(ctx->client_id, true, NULL);

    /* Set MQTT Protocol Version */
    int protocol_version = MQTT_PROTOCOL_V311;
    if (!strcmp(ctx->protocol_version, "mqttv31")) {
        protocol_version = MQTT_PROTOCOL_V31;
    } else if (!strcmp(ctx->protocol_version, "mqttv311")) {
        protocol_version = MQTT_PROTOCOL_V311;
    } else if (!strcmp(ctx->protocol_version, "mqttv5")) {
        protocol_version = MQTT_PROTOCOL_V5;
    }
    mosquitto_int_option(ctx->mosq, MOSQ_OPT_PROTOCOL_VERSION, protocol_version);

    /* Set Authentication if enabled */
    if (ctx->username != NULL && ctx->password != NULL) {
        ret = mosquitto_username_pw_set(ctx->mosq, ctx->username, ctx->password);
        if (ret) {
            if (ret == MOSQ_ERR_INVAL) {
                flb_plg_error(ctx->ins, "Error: Problem setting Authentication options: Invalid input parameters.");
            } else {
                flb_plg_error(ctx->ins, "Error: Problem setting Authentication options: %s.", mosquitto_strerror(ret));
            }
            mosquitto_lib_cleanup();
            return -1;
        }
    }

    /* Set TLS Settings if enabled */
    if (ctx->broker_tls) {
        ret = mosquitto_tls_set(ctx->mosq, ctx->broker_tls_ca_file, NULL, ctx->broker_tls_cert_file, ctx->broker_tls_key_file, NULL);
        if (ret) {
            if (ret == MOSQ_ERR_INVAL) {
                flb_plg_error(ctx->ins, "Error: Problem setting TLS options: File not found.");
            } else {
                flb_plg_error(ctx->ins, "Error: Problem setting TLS options: %s.", mosquitto_strerror(ret));
            }
            mosquitto_lib_cleanup();
            return -1;
        }
    }

    /* Connect to MQTT Broker */
    ret = mosquitto_connect(ctx->mosq, ctx->broker_host, ctx->broker_port, 60);
    if (ret > 0){
		if(ret == MOSQ_ERR_ERRNO) {
            flb_sds_t err = flb_sds_create(strerror(errno));
            flb_plg_error(ctx->ins, "Error while trying to connect to MQTT broker: %s.", err);
        } else {
            flb_plg_error(ctx->ins, "Error while trying to connect to MQTT broker: %s.", mosquitto_strerror(ret));
        }
        mosquitto_lib_cleanup();
        return -1;
    }

    ret = mosquitto_loop_start(ctx->mosq);
    if (ret > 0){
        flb_plg_error(ctx->ins, "Error while starting the MQTT loop.");
        mosquitto_lib_cleanup();
        return -1;
    }

    /* Export context */
    flb_output_set_context(ins, ctx);

    return 0;
}

static void cb_mqtt_flush(struct flb_event_chunk *event_chunk,
                             struct flb_output_flush *out_flush,
                             struct flb_input_instance *i_ins,
                             void *out_context,
                             struct flb_config *config)
{
    msgpack_unpacked result;
    size_t off = 0;
    struct flb_out_mqtt *ctx = out_context;
    (void) config;
    struct flb_time tmp;
    msgpack_object *p;

    msgpack_unpacked_init(&result);
    while (msgpack_unpack_next(&result,
                                event_chunk->data,
                                event_chunk->size, &off) == MSGPACK_UNPACK_SUCCESS) {
        flb_time_pop_from_msgpack(&tmp, &result, &p);

        /* Replace all placeholders with values in topic and payload */
        flb_sds_t topic_template = flb_sds_create_len(ctx->topic, strlen(ctx->topic));
        flb_sds_t topic = flb_sds_create_size(0);
        int ret = replace_all_placeholders(ctx, *p, topic_template, &topic, ctx->placeholder_names_topic, ctx->num_placeholders_topic, false);
        if (ret == FLB_ERROR) {
            return FLB_OUTPUT_RETURN(FLB_ERROR);
        }
        flb_sds_destroy(topic_template);

        flb_sds_t payload_template = flb_sds_create_len(ctx->payload, strlen(ctx->payload));
        flb_sds_t payload = flb_sds_create_size(0);
        ret = replace_all_placeholders(ctx, *p, payload_template, &payload, ctx->placeholder_names_payload, ctx->num_placeholders_payload, true);
        if (ret == FLB_ERROR) {
            return FLB_OUTPUT_RETURN(FLB_ERROR);
        }
        flb_sds_destroy(payload_template);

        /* Publish message to MQTT Broker */ 
        ret = mosquitto_publish(ctx->mosq, NULL, topic, strlen(payload), payload, ctx->qos, false);
        if(ret != MOSQ_ERR_SUCCESS) {
            flb_plg_error(ctx->ins, "Error while publishing message to MQTT broker: return code '%d'.", ret);
            return FLB_OUTPUT_RETURN(FLB_RETRY);
        }
                        
        /* Free up resources */
        flb_sds_destroy(topic);
        flb_sds_destroy(payload);
    }
    msgpack_unpacked_destroy(&result);

    FLB_OUTPUT_RETURN(FLB_OK);
}

static int cb_mqtt_exit(void *data, struct flb_config *config)
{
    struct flb_out_mqtt *ctx = data;

    if (!ctx) {
        return 0;
    }
    
    /* Close MQTT connection and free resources */
    mosquitto_disconnect(ctx->mosq);
    mosquitto_loop_stop(ctx->mosq, true);
    mosquitto_destroy(ctx->mosq);
    mosquitto_lib_cleanup();

    for (int i = 0; i < ctx->num_placeholders_topic; i++) {
        flb_free(ctx->placeholder_names_topic[i]);
    }
    flb_free(ctx->placeholder_names_topic);
    for (int i = 0; i < ctx->num_placeholders_payload; i++) {
        flb_free(ctx->placeholder_names_payload[i]);
    }
    flb_free(ctx->placeholder_names_payload);

    flb_free(ctx);
    return 0;
}

/* Configuration properties map */
static struct flb_config_map config_map[] = {
    {
     FLB_CONFIG_MAP_STR, "broker_host", NULL,
     0, FLB_TRUE, offsetof(struct flb_out_mqtt, broker_host),
    "The host of the broker to send messages to."
    },
    {
     FLB_CONFIG_MAP_INT, "broker_port", 0,
     0, FLB_TRUE, offsetof(struct flb_out_mqtt, broker_port),
    "The port on the broker to send messages to."
    },
    {
     FLB_CONFIG_MAP_STR, "topic", NULL,
     0, FLB_TRUE, offsetof(struct flb_out_mqtt, topic),
    "The MQTT topic name. It can include placeholders that will be replaced with values of the received JSON."
    },
    {
     FLB_CONFIG_MAP_STR, "payload", NULL,
     0, FLB_TRUE, offsetof(struct flb_out_mqtt, payload),
    "The desired MQTT payload structure. It can/should include placeholders that will be replaced with values of the received JSON."
    },
    // TODO: Set default to process ID
    {
     FLB_CONFIG_MAP_STR, "client_id", "fluentbit_xxx",
     0, FLB_TRUE, offsetof(struct flb_out_mqtt, client_id),
    "The ID to use for this MQTT client. Defaults to 'fluentbit_' appended with the process id."
    },
    {
     FLB_CONFIG_MAP_INT, "qos", 0,
     0, FLB_TRUE, offsetof(struct flb_out_mqtt, qos),
    "The quality of service level to use for all messages. Defaults to 0."
    },
    {
     FLB_CONFIG_MAP_STR, "protocol_version", "mqttv311",
     0, FLB_TRUE, offsetof(struct flb_out_mqtt, protocol_version),
    "The version of the MQTT protocol to use when connecting. Can be 'mqttv5', 'mqttv311' or 'mqttv31'. Defaults to 'mqttv311'."
    },
    {
     FLB_CONFIG_MAP_STR, "username", NULL,
     0, FLB_TRUE, offsetof(struct flb_out_mqtt, username),
    "The username to authenticate against the MQTT broker."
    },
    {
     FLB_CONFIG_MAP_STR, "password", NULL,
     0, FLB_TRUE, offsetof(struct flb_out_mqtt, password),
    "The password to authenticate against the MQTT broker."
    },
    {
     FLB_CONFIG_MAP_BOOL, "broker_tls", "false",
     0, FLB_TRUE, offsetof(struct flb_out_mqtt, broker_tls),
    "If TLS should be used to connect to MQTT broker."
    },
    {
     FLB_CONFIG_MAP_STR, "broker_tls_ca_file", NULL,
     0, FLB_TRUE, offsetof(struct flb_out_mqtt, broker_tls_ca_file),
    "The CA path and filename to connect to the MQTT broker via TLS."
    },
    {
     FLB_CONFIG_MAP_STR, "broker_tls_cert_file", NULL,
     0, FLB_TRUE, offsetof(struct flb_out_mqtt, broker_tls_cert_file),
    "The Cert path and filename to connect to the MQTT broker via TLS."
    },
    {
     FLB_CONFIG_MAP_STR, "broker_tls_key_file", NULL,
     0, FLB_TRUE, offsetof(struct flb_out_mqtt, broker_tls_key_file),
    "The Key path and filename to connect to the MQTT broker via TLS."
    },

    /* EOF */
    {0}
};

/* Plugin reference */
struct flb_output_plugin out_mqtt_plugin = {
    .name        = "mqtt",
    .description = "MQTT Output",
    .cb_init     = cb_mqtt_init,
    .cb_flush    = cb_mqtt_flush,
    .cb_exit     = cb_mqtt_exit,
    .event_type  = FLB_OUTPUT_LOGS,
    // .workers     = 2,
    .config_map   = config_map
};
