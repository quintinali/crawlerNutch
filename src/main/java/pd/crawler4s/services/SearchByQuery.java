package pd.crawler4s.services;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import pd.crawler4s.driver.ESdriver;

/**
 * Servlet implementation class SearchByQuery
 */
@WebServlet("/SearchByQuery")
public class SearchByQuery extends HttpServlet {
  private static final long serialVersionUID = 1L;

  /**
   * @see HttpServlet#HttpServlet()
   */
  public SearchByQuery() {
    super();
    // TODO Auto-generated constructor stub
  }

  /**
   * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
   *      response)
   */
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");
    String query = request.getParameter("query");
    String fromStr = request.getParameter("result_from");
    String limitStr = request.getParameter("result_limit");
    Integer limit = Integer.parseInt(limitStr);
    Integer from = Integer.parseInt(fromStr);

    ESdriver esd = (ESdriver) request.getServletContext().getAttribute("esd");
    String fileList = esd.searchByQuery(query, from, limit);
    PrintWriter out = response.getWriter();
    out.print(fileList);
    out.flush();
  }

  /**
   * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
   *      response)
   */
  protected void doPost(HttpServletRequest request,
      HttpServletResponse response) throws ServletException, IOException {
    // TODO Auto-generated method stub
    doGet(request, response);
  }

}
