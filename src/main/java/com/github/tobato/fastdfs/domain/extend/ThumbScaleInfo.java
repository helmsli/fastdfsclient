package com.github.tobato.fastdfs.domain.extend;

import java.io.Serializable;

public class ThumbScaleInfo implements Serializable {
	private float scale;
	private double outputQuality;
	private String cachedPrefixName;
	public float getScale() {
		return scale;
	}
	public void setScale(float scale) {
		this.scale = scale;
	}
	public double getOutputQuality() {
		return outputQuality;
	}
	public void setOutputQuality(double outputQuality) {
		this.outputQuality = outputQuality;
	}
	
	public String getCachedPrefixName() {
		return cachedPrefixName;
	}
	public void setCachedPrefixName(String cachedPrefixName) {
		this.cachedPrefixName = cachedPrefixName;
	}
	@Override
	public String toString() {
		return "SacleInfo [scale=" + scale + ", outputQuality=" + outputQuality + "]";
	}
	
}
