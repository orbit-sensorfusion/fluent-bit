set(src
  mqtt.c)

FLB_PLUGIN(out_mqtt "${src}" "")
target_link_libraries(flb-plugin-out_mqtt -lmosquitto)