package com.datageek.presto.udfs;

import com.datageek.presto.udfs.scalar.UDFRoundTime;
import com.facebook.presto.spi.Plugin;
import org.weakref.jmx.internal.guava.collect.ImmutableSet;
import java.util.Set;

public class functionPlugin implements Plugin
{
    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(UDFRoundTime.class)
                .build();
    }
}
