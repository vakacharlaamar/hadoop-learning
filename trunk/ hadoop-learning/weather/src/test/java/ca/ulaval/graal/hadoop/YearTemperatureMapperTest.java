package ca.ulaval.graal.hadoop;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class YearTemperatureMapperTest {
    @Mock 
    private Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context;
    private YearTemperatureMapper mapper;
    
    @Before
    public void setUp() {
        mapper = new YearTemperatureMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        mapper.map(new LongWritable(0), new Text("1900,1,30"), context);
        
        verify(context, times(1)).write(new IntWritable(1900), new IntWritable(30));
        
        verifyNoMoreInteractions(context);
    }
}
