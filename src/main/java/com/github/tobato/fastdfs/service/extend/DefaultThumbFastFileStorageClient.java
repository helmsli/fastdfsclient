package com.github.tobato.fastdfs.service.extend;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

import javax.annotation.Resource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.Validate;
import org.springframework.stereotype.Component;

import com.github.tobato.fastdfs.domain.MateData;
import com.github.tobato.fastdfs.domain.StorageNode;
import com.github.tobato.fastdfs.domain.StorePath;
import com.github.tobato.fastdfs.domain.ThumbImageConfig;
import com.github.tobato.fastdfs.domain.extend.ThumbImageScaleConfig;
import com.github.tobato.fastdfs.domain.extend.ThumbScaleInfo;
import com.github.tobato.fastdfs.domain.extend.ThumbSizeInfo;
import com.github.tobato.fastdfs.exception.FdfsUnsupportImageTypeException;
import com.github.tobato.fastdfs.exception.FdfsUploadImageException;
import com.github.tobato.fastdfs.proto.storage.StorageUploadSlaveFileCommand;
import com.github.tobato.fastdfs.service.DefaultFastFileStorageClient;

import net.coobird.thumbnailator.Thumbnails;

@Component
public class DefaultThumbFastFileStorageClient extends DefaultFastFileStorageClient
		implements ThumbFastFileStorageClient {
	@Resource
	private ThumbImageScaleConfig thumbImageScaleConfig;

	@Override
	public StorePath uploadAndCrtThumbImageByScales(InputStream inputStream, long fileSize, String fileExtName,
		Set<MateData> metaDataSet, List<ThumbScaleInfo> thumbScaleInfos) {
		List<ThumbScaleInfo> usedthumbScaleInfos = thumbScaleInfos;
		if(usedthumbScaleInfos==null)
		{
			usedthumbScaleInfos = thumbImageScaleConfig.getThumbScaleInfos();
			//thumbScaleInfos =  usedthumbScaleInfos;
		}
		Validate.notNull(inputStream, "上传文件流不能为空");
		Validate.notBlank(fileExtName, "文件扩展名不能为空");
		// 检查是否能处理此类图片
		if (!isSupportImage(fileExtName)) {
			throw new FdfsUnsupportImageTypeException("不支持的图片格式" + fileExtName);
		}
		StorageNode client = trackerClient.getStoreStorage();
		byte[] bytes = inputStreamToByte(inputStream);

		// 上传文件和mateData
		StorePath path = uploadFileAndMateData(client, new ByteArrayInputStream(bytes), fileSize, fileExtName,
				metaDataSet);
		// 上传缩略图
		for (int i = 0; i < usedthumbScaleInfos.size(); i++) {
			uploadThumbImageScale(client, new ByteArrayInputStream(bytes), path.getPath(), fileExtName,
					usedthumbScaleInfos.get(i));
		}
		bytes = null;
		return path;

	}

	@Override
	public StorePath uploadAndCrtThumbImageBySize(InputStream inputStream, long fileSize, String fileExtName,
			Set<MateData> metaDataSet, List<ThumbSizeInfo> thumbSizeInfos) {
		Validate.notNull(inputStream, "上传文件流不能为空");
		Validate.notBlank(fileExtName, "文件扩展名不能为空");
		// 检查是否能处理此类图片
		if (!isSupportImage(fileExtName)) {
			throw new FdfsUnsupportImageTypeException("不支持的图片格式" + fileExtName);
		}
		StorageNode client = trackerClient.getStoreStorage();
		byte[] bytes = inputStreamToByte(inputStream);

		// 上传文件和mateData
		StorePath path = uploadFileAndMateData(client, new ByteArrayInputStream(bytes), fileSize, fileExtName,
				metaDataSet);
		// 上传缩略图
		for (int i = 0; i < thumbSizeInfos.size(); i++) {
			uploadThumbImageSize(client, new ByteArrayInputStream(bytes), path.getPath(), fileExtName,
					thumbSizeInfos.get(0));
		}
		bytes = null;
		return path;

	}

	/**
	 * 按照固定的尺寸压缩
	 * 
	 * @param inputStream
	 * @param thumbSizeInfo
	 * @return
	 * @throws IOException
	 */
	protected ByteArrayInputStream getThumbImageStreamSize(InputStream inputStream, ThumbSizeInfo thumbSizeInfo)
			throws IOException {
		// 在内存当中生成缩略图
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		// @formatter:off
		Thumbnails.of(inputStream).size(thumbSizeInfo.getWidth(), thumbSizeInfo.getHeight()).toOutputStream(out);
		// @formatter:on
		return new ByteArrayInputStream(out.toByteArray());
	}

	protected void uploadThumbImageSize(StorageNode client, InputStream inputStream, String masterFilename,
			String fileExtName, ThumbSizeInfo thumbSizeInfo) {
		ByteArrayInputStream thumbImageStream = null;
		try {
			thumbImageStream = getThumbImageStreamSize(inputStream, thumbSizeInfo);// getFileInputStream
			// 获取文件大小
			long fileSize = thumbImageStream.available();
			// 获取缩略图前缀
			String prefixName = thumbSizeInfo.getCachedPrefixName();
			StorageUploadSlaveFileCommand command = new StorageUploadSlaveFileCommand(thumbImageStream, fileSize,
					masterFilename, prefixName, fileExtName);
			connectionManager.executeFdfsCmd(client.getInetSocketAddress(), command);

		} catch (IOException e) {
			LOGGER.error("upload ThumbImage error", e);
			throw new FdfsUploadImageException("upload ThumbImage error", e.getCause());
		} finally {
			IOUtils.closeQuietly(thumbImageStream);
		}
	}

	// 等比例压缩
	protected ByteArrayInputStream getThumbImageStreamScale(InputStream inputStream, ThumbScaleInfo thumbScaleInfo)
			throws IOException {
		// 在内存当中生成缩略图
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		// @formatter:off
		Thumbnails.of(inputStream).scale(thumbScaleInfo.getScale()).outputQuality(thumbScaleInfo.getOutputQuality())
				.toOutputStream(out);
		// @formatter:on
		return new ByteArrayInputStream(out.toByteArray());
	}

	protected void uploadThumbImageScale(StorageNode client, InputStream inputStream, String masterFilename,
			String fileExtName, ThumbScaleInfo thumbScaleInfo) {
		ByteArrayInputStream thumbImageStream = null;
		try {
			thumbImageStream = getThumbImageStreamScale(inputStream, thumbScaleInfo);// getFileInputStream
			// 获取文件大小
			long fileSize = thumbImageStream.available();
			// 获取缩略图前缀
			String prefixName = thumbScaleInfo.getCachedPrefixName();
			StorageUploadSlaveFileCommand command = new StorageUploadSlaveFileCommand(thumbImageStream, fileSize,
					masterFilename, prefixName, fileExtName);
			connectionManager.executeFdfsCmd(client.getInetSocketAddress(), command);

		} catch (IOException e) {
			LOGGER.error("upload ThumbImage error", e);
			throw new FdfsUploadImageException("upload ThumbImage error", e.getCause());
		} finally {
			IOUtils.closeQuietly(thumbImageStream);
		}
	}

	@Override
	public StorePath uploadAndCrtThumbImageByAuto(InputStream inputStream, long fileSize, String fileExtName,
			Set<MateData> metaDataSet) {
			List<ThumbScaleInfo> usedthumbScaleInfos  = thumbImageScaleConfig.getThumbScaleInfos();
				//thumbScaleInfos =  usedthumbScaleInfos;
			
			Validate.notNull(inputStream, "上传文件流不能为空");
			Validate.notBlank(fileExtName, "文件扩展名不能为空");
			// 检查是否能处理此类图片
			if (!isSupportImage(fileExtName)) {
				throw new FdfsUnsupportImageTypeException("不支持的图片格式" + fileExtName);
			}
			StorageNode client = trackerClient.getStoreStorage();
			byte[] bytes = inputStreamToByte(inputStream);

			// 上传文件和mateData
			StorePath path = uploadFileAndMateData(client, new ByteArrayInputStream(bytes), fileSize, fileExtName,
					metaDataSet);
			// 上传缩略图
			for (int i = 0; i < usedthumbScaleInfos.size(); i++) {
				uploadThumbImageScale(client, new ByteArrayInputStream(bytes), path.getPath(), fileExtName,
						usedthumbScaleInfos.get(i));
			}
			List<ThumbSizeInfo> usedThumbSizeInfo  = thumbImageScaleConfig.getThumbScaleSizes();
			
			for (int i = 0; i < usedThumbSizeInfo.size(); i++) {
				uploadThumbImageSize(client, new ByteArrayInputStream(bytes), path.getPath(), fileExtName,
						usedThumbSizeInfo.get(i));
			}
			bytes = null;
			return path;

	}
}
