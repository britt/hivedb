package org.hivedb.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class GetOptTest {

	@Test
	public void testArgumentMap() {
		String format = "f:,classpath:,verbose";
		GetOpt opt = new GetOpt(format);
		
		Map<String,String> argz = opt.toMap(new String[] {"aCommand","-classpath", "aClasspath", "--f", "aFile", "--verbose"});
		Assert.assertEquals(argz.size(), 4);
		Assert.assertTrue(argz.containsKey("classpath"));
		Assert.assertTrue(argz.containsKey("f"));
		Assert.assertTrue(argz.containsKey("verbose"));
		Assert.assertFalse("".equals(argz.get("classpath")));
		Assert.assertFalse("".equals(argz.get("f")));
		Assert.assertEquals(argz.get("verbose"), "");
		Assert.assertEquals(argz.get("0"), "aCommand");
	}
	
	@Test
	public void testValidation() {
		String format = "f:,classpath:,verbose";
		GetOpt opt = new GetOpt(format);
		
		opt.toMap(new String[] {"aCommand","-classpath", "aClasspath", "--f", "aFile", "--verbose"});
		Assert.assertTrue(opt.validate());
	}
}
