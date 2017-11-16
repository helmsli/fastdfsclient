package com.github.tobato.fastdfs.domain.extend;

import java.util.List;

public interface ThumbImageScaleConfig {
	public List<ThumbScaleInfo> getThumbScaleInfos();
	
	public List<ThumbSizeInfo> getThumbSizes();
	
	public int getUploadOriginalImage();
}
