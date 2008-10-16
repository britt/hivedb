package org.hivedb.util;

import org.hivedb.util.functional.Factory;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JMock.class)
public class SynchronizedSingletonProviderTest {
  private Mockery mockery;
  private Factory<Integer> factory;

  @Before
  public void setUp() throws Exception {
    mockery = new JUnit4Mockery() {
      {
        //setImposteriser(ClassImposteriser.INSTANCE);
      }
    };
    factory = mockery.mock(Factory.class);
  }

  @Test
  public void shouldSwapOutTheInstanceWhenItObservesAnUpdate() throws Exception {
    mockery.checking(new Expectations() {
      {
        one(factory).newInstance();
        will(returnValue(1));
        one(factory).newInstance();
        will(returnValue(2));
      }
    });
    SynchronizedSingletonProvider<Integer> provider = new SynchronizedSingletonProvider<Integer>(factory);
    assertEquals(new Integer(1), provider.getSynchronizedInstance().get());
    provider.update(null, null);
    assertEquals(new Integer(2), provider.getSynchronizedInstance().get());
  }
}
