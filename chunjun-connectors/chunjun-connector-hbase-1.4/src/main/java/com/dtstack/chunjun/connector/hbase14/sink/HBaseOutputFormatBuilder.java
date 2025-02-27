/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.hbase14.sink;

import com.dtstack.chunjun.connector.hbase.conf.HBaseConfigConstants;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormatBuilder;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

/**
 * The Builder class of HbaseOutputFormatBuilder
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class HBaseOutputFormatBuilder extends BaseRichOutputFormatBuilder<HBaseOutputFormat> {

    public HBaseOutputFormatBuilder() {
        super(new HBaseOutputFormat());
    }

    public void setTableName(String tableName) {
        format.setTableName(tableName);
    }

    public void setHbaseConfig(Map<String, Object> hbaseConfig) {
        format.setHbaseConfig(hbaseConfig);
    }

    public void setWriteBufferSize(Long writeBufferSize) {
        if (writeBufferSize == null || writeBufferSize == 0L) {
            format.setWriteBufferSize(HBaseConfigConstants.DEFAULT_WRITE_BUFFER_SIZE);
        } else {
            format.setWriteBufferSize(writeBufferSize);
        }
    }

    @Override
    protected void checkFormat() {
        Preconditions.checkArgument(StringUtils.isNotEmpty(format.getTableName()));
        Preconditions.checkNotNull(format.getHbaseConfig());
    }
}
