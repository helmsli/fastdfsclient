package com.github.tobato.fastdfs.service.extend;

import java.io.InputStream;
import java.util.List;
import java.util.Set;

import com.github.tobato.fastdfs.domain.MateData;
import com.github.tobato.fastdfs.domain.StorePath;
import com.github.tobato.fastdfs.domain.extend.ThumbScaleInfo;
import com.github.tobato.fastdfs.domain.extend.ThumbSizeInfo;
import com.github.tobato.fastdfs.service.FastFileStorageClient;

public interface ThumbFastFileStorageClient extends FastFileStorageClient {
	StorePath uploadAndCrtThumbImageByScales(InputStream inputStream, long fileSize, String fileExtName,
            Set<MateData> metaDataSet,List<ThumbScaleInfo> thumbScaleInfos);

	StorePath uploadAndCrtThumbImageBySize(InputStream inputStream, long fileSize, String fileExtName,
            Set<MateData> metaDataSet,List<ThumbSizeInfo> thumbSizeInfoa);

	StorePath uploadAndCrtThumbImageByAuto(InputStream inputStream, long fileSize, String fileExtName,
            Set<MateData> metaDataSet);
	
	public StorePath uploadString(String content, String fileExtension);
}
