package pd.nutch.services;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import pd.nutch.driver.ESdriver;

/**
 * Application Lifecycle Listener implementation class NutchListener
 *
 */
@WebListener
public class NutchListener implements ServletContextListener {
  ESdriver esd = null;

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
    if (esd != null) {
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
  public void contextInitialized(ServletContextEvent arg0) {
    ServletContext ctx = arg0.getServletContext();
    esd = new ESdriver();
    ctx.setAttribute("esd", esd);
  }

}
