package com.wdcloud.graphx.javaUtil;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

//import net.librec.conf.Configurable;

public class ReflectionUtil {
    private static final Class<?>[] EMPTY_ARRAY = new Class[]{};
    /**
     * Cache of constructors for each class. Pins the classes so they can't be
     * garbage collected until ReflectionUtils can be collected.
     * 每个类的构造函数的缓存。 引导类，所以他们不能被垃圾收集，直到ReflectionUtils可以收集。
     */
    private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE = new ConcurrentHashMap<Class<?>, Constructor<?>>();

    /**
     * Create an object for the given class and initialize it from conf
     *
     * @param <T> type parameter
     * @param theClass   a given Class object
     * @param paramClass Class type of the constructor(指定参数的Class对象)
     * @param paramValue object for the constructor
     * @return a new object
     */
    @SuppressWarnings("unchecked")
    public static <T> T newInstance(Class<T> theClass, Class<?> paramClass, Object paramValue) {
        T result;
        try {
            Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
            if (meth == null) {
                meth = theClass.getDeclaredConstructor(paramClass);
                meth.setAccessible(true);
                CONSTRUCTOR_CACHE.put(theClass, meth);
            }
            result = meth.newInstance(paramValue);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }
  
    
    /**
     * Create an object for the given class and initialize it from conf
     *
     * @param <T> type parameter
     * @param theClass class of which an object is created
     * @param conf     Configuration
     * @return a new object
     */
//    public static <T> T newInstance(Class<T> theClass, Configuration conf) {
//    	//根据传进来的class创建对应的对象
//        T result = newInstance(theClass);
//        //Check and set 'configuration' if necessary.
//        //setConf(result, conf);
//        return result;
//    }

    /*
     * 根据传进来的class创建对应的对象，并返回
     */
    @SuppressWarnings("unchecked")
    public static <T> T newInstance(Class<T> theClass) {
        T result;
        try {
        	//CONSTRUCTOR_CACHE： Cache of constructors for each class
            Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
            if (meth == null) {
            	//return the Constructor object for the constructor with the specified parameter list 
            	//返回具有指定参数列表的构造函数的构造函数对象(但是这里的构造参数为空)
                meth = theClass.getDeclaredConstructor(EMPTY_ARRAY);
                //将此对象的可访问标志设置为指定的布尔值
                meth.setAccessible(true);
                //将对应theClass和其对应的构造器缓存到CONSTRUCTOR_CACHE中
                CONSTRUCTOR_CACHE.put(theClass, meth);
            }
            //创建theClass类对应的对象
            result = meth.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    /**
     * Check and set 'configuration' if necessary.
     *
     * @param theObject object for which to set configuration
     * @param conf      Configuration
     */
//    public static void setConf(Object theObject, Configuration conf) {
//        if (conf != null) {
//            if (theObject instanceof Configurable) {
//                ((Configurable) theObject).setConf(conf);
//            }
//        }
//    }

}
