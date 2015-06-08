package com.alibaba.rocketmq.storm.internal.tools;

import com.alibaba.rocketmq.storm.model.CRLog;

/**
 * Created by robert on 2015/6/8.
 */
public class CRLogUtils {

    public static boolean checkCRLog(CRLog crLog){
        return checkOffId(crLog.getOffer_id())&&checkAffId(crLog.getAffiliate_id())&&checkEventCode(crLog.getEvent_code());
    }

    private static boolean checkOffId(String offId){
        boolean result = true;
        if (null == offId)
            result = false;
        return result;
    }

    private static boolean checkAffId(String affId){
        boolean result = true;
        if (null == affId)
            result = false;
        return result;
    }

    private static boolean checkEventCode(String eventCode){
        boolean result = true;
        if (null == eventCode)
            result = false;
        return result;
    }
}
