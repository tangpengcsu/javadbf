package com.linuxense.javadbf;

import java.util.Map;

/**
 * 版权声明：本程序模块属于后台业务系统（FSPT）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：
 * 模块描述：
 * 开发作者：tang.peng
 * 创建日期：2019/6/29
 * 模块版本：1.0.0.0
 * ----------------------------------------------------------------
 * 修改日期      版本       作者           备注
 * 2019/6/29   1.0.0.0    tang.peng     创建
 * ----------------------------------------------------------------
 */
public class DBFSkipRow extends DBFRow {
    public DBFSkipRow(Object[] data, Map<String, Integer> mapcolumnNames, DBFField[] fields) {
        super(data, mapcolumnNames, fields);
    }
}
