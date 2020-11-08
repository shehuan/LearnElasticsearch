package com.sn.elasticsearch.service;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class ParseHtmlService {
    public void parse() {
        Map<String, String> headers = new HashMap<>();
        headers.put("cookie", "__jdu=1019675532; shshshfpa=f869d8e2-69b9-358e-5dcb-477e8ef9482d-1592724368; shshshfpb=itCeNyfLZd4q0XEuY4ktKAQ%3D%3D; qrsc=3; rkv=1.0; ipLoc-djd=27-2376-2381-0; areaId=27; user-key=ba2c2f27-874b-440c-adef-2fd1d3fa2c7b; TrackID=1ISOZJemvKrBd9TLBsXzdWqMm46MVUhtg4v_nvQ_QQNDaxCFHL_4NX-dEWoV_xuoQfkW0cs-MCjoCyHmNPGXv_JdGjcbdWVEm2rvt5NQBqjTZLek4cCVCgxLEl1sULgkO; pinId=qYIsSFyBW4wlXdeRkXF5A2MUPWT6-mAV; pin=%E4%BE%9D%E7%84%B6%E8%8C%83%E7%89%B9%E8%A5%BFSH; unick=SheHuannn; ceshi3.com=201; _tp=mVV%2BvIxF36NRh0bwXdTccfTGGubTI%2FqluhhWPdnWLrJhG%2FXHI3O%2BIY080h22%2Btjo; _pst=%E4%BE%9D%E7%84%B6%E8%8C%83%E7%89%B9%E8%A5%BFSH; unpl=V2_ZzNtbUFQR0cnChFdfkpYDWIEFVkRUBAScA5FXHMZXQI3BUIOclRCFnQURlVnG1wUZAMZXUNcQRNFCEdkeBBVAWMDE1VGZxBFLV0CFSNGF1wjU00zQwBBQHcJFF0uSgwDYgcaDhFTQEJ2XBVQL0oMDDdRFAhyZ0AVRQhHZHsYXA1gBRZZQFRzJXI4dmRzH1wDZAIiXHJWc1chVE9UfBheSGcCElVFUUcRdwt2VUsa; __jdv=76161171|baidu-pinzhuan|t_288551095_baidupinzhuan|cpc|0f3d30c8dba7459bb52f2eb5eba8ac7d_0_265cc3f84b594665b6b647299106a7ab|1604149836557; cn=6; shshshfp=2939b23fe88aed5b21d8a1079fee095a; __jda=122270672.1019675532.1592724364.1604149771.1604841540.7; __jdc=122270672; 3AB9D23F7A4B3C9B=TMJSU2H6IHLA72TFXCENIWG2RLB2XS6DOQRSY5ZZ2TPJDB74PA5VRTJ4Y4MCYZKV2R5XTXLR3FJT337JXXTAYZEURE; wlfstk_smdl=u7t6rp7e9lqiwy5muw9o2m04oxuhp7nk; __jdb=122270672.9.1019675532|7.1604841540; shshshsID=a0427fc9f173d34e7f2704055dbc7b8f_7_1604843336659");
        headers.put("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36");

        int page = 1;
        int total = 0;
        while (true) {
            String url = "https://search.jd.com/Search?keyword=java&page=" + page + "&s=1&click=0";
            try {
                Document document = Jsoup.connect(url).timeout(10 * 1000).headers(headers).get();
                Elements list = document.select("ul.gl-warp.clearfix > li > div.gl-i-wrap");
                if (list.size() == 0) {
                    break;
                }
                for (Element item : list) {
                    String title = item.select("div.p-name.p-name-type-2 > a > em").get(0).text();
                    System.out.println(title);
                }
                total += list.size();
                System.out.println("page==" + page);
                System.out.println("已采集数据条数==" + total);
                ++page;
                Thread.sleep(2 * 1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
