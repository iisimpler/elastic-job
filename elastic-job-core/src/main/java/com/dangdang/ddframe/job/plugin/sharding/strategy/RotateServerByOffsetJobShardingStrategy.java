package com.dangdang.ddframe.job.plugin.sharding.strategy;

import com.dangdang.ddframe.job.internal.sharding.strategy.JobShardingStrategy;
import com.dangdang.ddframe.job.internal.sharding.strategy.JobShardingStrategyOption;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 根据作业名的哈希值对服务器列表进行轮转的分片策略.
 * 
 * Created by xiangru.meng on 2018/9/13.
 */
public class RotateServerByOffsetJobShardingStrategy implements JobShardingStrategy {
    
    private AverageAllocationJobShardingStrategy averageAllocationJobShardingStrategy = new AverageAllocationJobShardingStrategy();
    
    @Override
    public Map<String, List<Integer>> sharding(final List<String> serversList, final JobShardingStrategyOption option) {
        return averageAllocationJobShardingStrategy.sharding(rotateServerList(serversList, option.getJobName(), option.getOffset()), option);
    }
    
    private List<String> rotateServerList(final List<String> serversList, final String jobName, final Integer offset) {
        int serverSize = serversList.size();
        Integer offset2 = offset;

        if (offset2 == null || offset2 == 0) {
            offset2 = Math.abs(jobName.hashCode()) % serverSize;
        }

        if (offset2 == 0) {
            return serversList;
        }

        List<String> result = new ArrayList<>(serverSize);
        for (int i = 0; i < serverSize; i++) {
            int index = (i + offset2) % serverSize;
            result.add(serversList.get(index));
        }
        return result;
    }
}
