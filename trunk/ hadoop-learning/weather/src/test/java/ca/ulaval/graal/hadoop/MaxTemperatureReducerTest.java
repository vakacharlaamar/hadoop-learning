package ca.ulaval.graal.hadoop;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class MaxTemperatureReducerTest {
	@Mock
	private Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context;
	private MaxTemperatureReducer reducer;

	@Before
	public void setUp() {
		reducer = new MaxTemperatureReducer();
	}

	@Test
	public void testReduceIntWritableIterableOfIntWritableContext()
			throws Exception {
		IntWritable key = new IntWritable(2012);
		IntWritable maxTemp = new IntWritable(30);
		List<IntWritable> values = Lists.newArrayList(maxTemp);

		reducer.reduce(key, values, context);

		verify(context, times(1)).write(key, maxTemp);

		verifyNoMoreInteractions(context);
	}

}
