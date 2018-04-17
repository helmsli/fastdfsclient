package com.github.tobato.fastdfs.domain.extend;

import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import com.github.tobato.fastdfs.FdfsClientConstants;

@Component
@ConfigurationProperties(prefix = "fdfs.thumb-image-scale")
public class DefaultThumbImageScaleConfig implements ThumbImageScaleConfig {

	
	private List<String> thumbScaleList = new ArrayList<String>();
	 
	
	private List<String> thumbSizeList = new ArrayList<String>();
		 

	private  List<ThumbScaleInfo> thumbScales = new ArrayList<ThumbScaleInfo>();

	private  List<ThumbSizeInfo> thumbSizes = new ArrayList<ThumbSizeInfo>();

	@Override
	public List<ThumbScaleInfo> getThumbScaleInfos() {
		// TODO Auto-generated method stub
		if(thumbScales.size()==0&&thumbScaleList.size()>0)
		{
			synchronized(this)
			{
				if(thumbScales.size()==0)
				{
					for (int i = 0; i < thumbScaleList.size(); i++) {
						String[] contents = StringUtils.split(thumbScaleList.get(i), ":");
						if (contents.length >= 3) {
							ThumbScaleInfo thumbScaleInfo = new ThumbScaleInfo();
							thumbScaleInfo.setCachedPrefixName(contents[0]);
							thumbScaleInfo.setScale(Float.parseFloat(contents[1]));
							thumbScaleInfo.setOutputQuality(Double.parseDouble(contents[2]));
							thumbScales.add(thumbScaleInfo);
						}
					}
				}
			}
			
		}
		return thumbScales;
	}

	public List<String> getThumbScaleList() {
		return thumbScaleList;
	}

	
	
	public List<String> getThumbSizeList() {
		return thumbSizeList;
	}

	public void setThumbSizeList(List<String> thumbSizeList) {
		this.thumbSizeList = thumbSizeList;
		this.thumbSizes.clear();
	}

	public void setThumbScaleList(List<String> thumbScaleList) {
		this.thumbScaleList = thumbScaleList;
		this.thumbScales.clear();
	}//end function

	@Override
	public List<ThumbSizeInfo> getThumbScaleSizes() {
		// TODO Auto-generated method stub
		if(this.thumbSizes.size()==0&&this.thumbSizeList.size()>0)
		{
			synchronized(this)
			{
				if(thumbSizes.size()==0)
				{
					for (int i = 0; i < thumbSizeList.size(); i++) {
						String[] contents = StringUtils.split(thumbSizeList.get(i), ":");
						if (contents.length >= 3) {
							ThumbSizeInfo thumbSizeInfo = new ThumbSizeInfo();
							thumbSizeInfo.setCachedPrefixName(contents[0]);
							thumbSizeInfo.setHeight(Integer.parseInt(contents[1]));
							thumbSizeInfo.setWidth(Integer.parseInt(contents[2]));
							thumbSizes.add(thumbSizeInfo);
						}
					}
				}
			}
			
		}
		return thumbSizes;
	}
}
