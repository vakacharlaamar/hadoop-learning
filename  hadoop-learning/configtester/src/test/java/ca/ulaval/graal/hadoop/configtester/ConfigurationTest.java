package ca.ulaval.graal.hadoop.configtester;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

public class ConfigurationTest {
	private Configuration conf = new Configuration();
	
	@Before
	public void before() {
		conf.addResource("configuration-1.xml");
	}
	
	@Test
	public void colorIsDefined() {
		assertThat(conf.get("color"), is("yellow"));
	}
	
	@Test
	public void weightIsDefined() {
		assertThat(conf.get("weight"), is("heavy"));
	}
	
	@Test
	public void sizeIsDefined() {
		assertThat(conf.getInt("size",0), is(10));
	}
}
