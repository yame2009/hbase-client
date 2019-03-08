package com.mop.springcloud.corebase.demo;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.byxf.hbase.dao.HBaseDao;

/**
 * Author: Mr.tan
 * Date:  2017/09/05
 */

@RestController
@RequestMapping("/welcome")
public class WelcomeController {

    @Autowired
    private HBaseDao hBaseDao;

    @RequestMapping("hbase-demo")
    public String testHbaseDemo() {
        List<String> list = null;
        try {
            list = hBaseDao.getRowKeys("mopnovel_favoriteindex");
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println(list);
		// System.out.println(HBaseFactoryBean.getSpecifyConnection(1));

        try {
            System.out.println(hBaseDao.get("mop_articles_desc","7703046885167257003",Article.class));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "hello";
    }
}
