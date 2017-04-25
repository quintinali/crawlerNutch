package pd.nutch.services;

import java.util.Properties;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import pd.nutch.driver.ESDriver;
import pd.nutch.driver.SparkDriver;
import pd.nutch.main.CrawlerEngine;
import pd.nutch.ranking.Ranker;
import pd.nutch.ranking.Searcher;

/**
 * Application Lifecycle Listener implementation class NutchListener
 *
 */
@WebListener
public class NutchListener implements ServletContextListener {
	CrawlerEngine me = null;
	//ESDriver esd = null;

	/**
	 * Default constructor.
	 */
	public NutchListener() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see ServletContextListener#contextDestroyed(ServletContextEvent)
	 */
	public void contextDestroyed(ServletContextEvent arg0) {
		if (me != null) {
			me.end();
		}
	}

	/**
	 * @see ServletContextListener#contextInitialized(ServletContextEvent)
	 */
	public void contextInitialized(ServletContextEvent arg0) {
		me = new CrawlerEngine();
		Properties config = me.loadConfig();
		me.setES(new ESDriver(config));
		SparkDriver spark = new SparkDriver(config);

		ServletContext ctx = arg0.getServletContext();
		Searcher sr = new Searcher(me.getConfig(), me.getES(), null);
		Ranker rr = new Ranker(me.getConfig(), me.getES(), spark);
		ctx.setAttribute("CrawlerInstance", me);
		ctx.setAttribute("CrawlerSearcher", sr);
		ctx.setAttribute("CrawlerRanker", rr);

		//ServletContext ctx = arg0.getServletContext();
		//esd = new ESDriver();
		//ctx.setAttribute("esd", esd);
	}

}
