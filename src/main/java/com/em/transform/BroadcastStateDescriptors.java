package com.em.transform;


import com.em.pojo.Rule;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

public class BroadcastStateDescriptors {

    public static final MapStateDescriptor<Void, Rule> RULES_BROADCAST_STATE_DESCRIPTOR = new MapStateDescriptor<>(
            "RulesBroadcastState",
            Types.VOID,
            TypeInformation.of(new TypeHint<Rule>() {})
    );
}