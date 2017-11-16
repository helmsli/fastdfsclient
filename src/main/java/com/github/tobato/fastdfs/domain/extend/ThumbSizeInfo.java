package com.github.tobato.fastdfs.domain.extend;

import java.io.Serializable;

public class ThumbSizeInfo implements Serializable {
	private int width;

	private int height;

	private String cachedPrefixName;

	public int getWidth() {
		return width;
	}

	public void setWidth(int width) {
		this.width = width;
	}

	public int getHeight() {
		return height;
	}

	public void setHeight(int height) {
		this.height = height;
	}

	public String getCachedPrefixName() {
		return cachedPrefixName;
	}

	public void setCachedPrefixName(String cachedPrefixName) {
		this.cachedPrefixName = cachedPrefixName;
	}

}
