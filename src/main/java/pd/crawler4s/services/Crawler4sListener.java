package pd.crawler4s.services;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import pd.crawler4s.driver.ESdriver;


/**
 * Application Lifecycle Listener implementation class Crawler4sListener
 *
 */
@WebListener
public class Crawler4sListener implements ServletContextListener {
  ESdriver esd = null;
  /**
   * Default constructor. 
   */
  public Crawler4sListener() {
    // TODO Auto-generated constructor stub
  }

  /**
   * @see ServletContextListener#contextDestroyed(ServletContextEvent)
   */
  public void contextDestroyed(ServletContextEvent arg0)  { 
    if(esd!=null)
    {
      try {
        esd.closeES();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * @see ServletContextListener#contextInitialized(ServletContextEvent)
   */
  public void contextInitialized(ServletContextEvent arg0)  {    
    ServletContext ctx = arg0.getServletContext();
    esd = new ESdriver();
    ctx.setAttribute("esd", esd);
  }

}
