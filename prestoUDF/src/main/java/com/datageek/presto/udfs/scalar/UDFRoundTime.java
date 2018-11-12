package com.datageek.presto.udfs.scalar;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.regex.Pattern;


public class UDFRoundTime {
    private final static Pattern PATTERN_COLON = Pattern.compile(":");
    private final static Pattern PATTERN_SPACE = Pattern.compile(" ");

    @Description("Round time with one parameter, interval uses 5 as default")
    @ScalarFunction("ud_round_time")
    @SqlType(StandardTypes.VARCHAR)
    public Slice udfRoundTime1(@SqlType(StandardTypes.VARCHAR) Slice string) {
        return udfRoundTime(string, 5);
    }

    @Description("Round time with two parameters: datetime string and interval")
    @ScalarFunction("ud_round_time")
    @SqlType(StandardTypes.VARCHAR)
    public Slice udfRoundTime(@SqlType(StandardTypes.VARCHAR) Slice dateTimeStr, @SqlType(StandardTypes.BIGINT)  long interval) {
        String res = "";
        int min = 0;
        boolean isErr = false;
        try {
            String str = dateTimeStr.toStringUtf8().trim();
            if (str.contains(" ")) {
                String[] datetime = PATTERN_SPACE.split(str);
                res += datetime[0];
                String[] hms = PATTERN_COLON.split(datetime[1].trim());
                res += " " + hms[0] + ":";
                min = Integer.parseInt(hms[1]);
            } else {
                String[] hms = PATTERN_COLON.split(str);
                res += hms[0] + ":";
                min = Integer.parseInt(hms[1]);
            }
        } catch (Exception e) {
            isErr = true;
        }
        if (isErr == true) {
            return dateTimeStr;
        } else {
            min = min - min % (int)interval;
            res += String.format("%02d",min) + ":00";
            return Slices.utf8Slice(res);
        }
    }

}
