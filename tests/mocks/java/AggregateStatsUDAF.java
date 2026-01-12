package com.company.etl.udafs;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * Hive UDAF (User Defined Aggregate Function) to calculate statistics.
 * 
 * Usage:
 *   CREATE TEMPORARY FUNCTION calc_stats AS 'com.company.etl.udafs.AggregateStatsUDAF';
 *   SELECT region, calc_stats(sales_amount) FROM sales.transactions GROUP BY region;
 */
public class AggregateStatsUDAF extends UDAF {
    
    public static class StatsState {
        double sum = 0;
        double count = 0;
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
    }
    
    public static class StatsEvaluator implements UDAFEvaluator {
        private StatsState state;
        
        public void init() {
            state = new StatsState();
        }
        
        public boolean iterate(double value) {
            if (state == null) {
                state = new StatsState();
            }
            state.sum += value;
            state.count++;
            state.min = Math.min(state.min, value);
            state.max = Math.max(state.max, value);
            return true;
        }
        
        public StatsState terminatePartial() {
            return state;
        }
        
        public boolean merge(StatsState other) {
            if (other != null) {
                state.sum += other.sum;
                state.count += other.count;
                state.min = Math.min(state.min, other.min);
                state.max = Math.max(state.max, other.max);
            }
            return true;
        }
        
        public String terminate() {
            if (state.count == 0) {
                return null;
            }
            double avg = state.sum / state.count;
            return String.format("avg:%.2f,min:%.2f,max:%.2f,count:%.0f", 
                               avg, state.min, state.max, state.count);
        }
    }
}

