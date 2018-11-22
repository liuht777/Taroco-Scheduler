package io.github.liuht777.scheduler.core;

import org.springframework.util.ReflectionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;

/**
 * Runnable 封装 提供自定义属性
 * 对定时任务类采用反射调用
 *
 * @author liuht
 */
public class ScheduledMethodRunnable implements Runnable {

	private final Object target;

	private final Method method;

	private final String params;

	private final String extKeySuffix;


	public ScheduledMethodRunnable(Object target, Method method, String params, String extKeySuffix) {
		this.target = target;
		this.method = method;
		this.params = params;
		this.extKeySuffix = extKeySuffix;
	}

	public ScheduledMethodRunnable(Object target, String methodName, String params, String extKeySuffix) throws NoSuchMethodException {
		this.target = target;
		this.method = target.getClass().getMethod(methodName);
		this.params = params;
		this.extKeySuffix = extKeySuffix;
	}


	public Object getTarget() {
		return this.target;
	}

	public Method getMethod() {
		return this.method;
	}

	public String getParams() {
		return params;
	}

	public String getExtKeySuffix() {
		return extKeySuffix;
	}

	@Override
	public void run() {
		try {
			ReflectionUtils.makeAccessible(this.method);
			if(this.getParams() != null){
				this.method.invoke(this.target, this.getParams());
			}else{
				this.method.invoke(this.target);
			}
		}
		catch (InvocationTargetException ex) {
			ReflectionUtils.rethrowRuntimeException(ex.getTargetException());
		}
		catch (IllegalAccessException ex) {
			throw new UndeclaredThrowableException(ex);
		}
	}

}
