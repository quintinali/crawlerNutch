package pd.nutch.services;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import pd.nutch.driver.ESDriver;
import pd.nutch.main.CrawlerConstants;
import pd.nutch.main.CrawlerEngine;
import pd.nutch.ranking.Ranker;
import pd.nutch.ranking.Searcher;

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

		CrawlerEngine engine = (CrawlerEngine) request.getServletContext().getAttribute("CrawlerInstance");
		Searcher sr = (Searcher) request.getServletContext().getAttribute("CrawlerSearcher");	
		Ranker rr = (Ranker) request.getServletContext().getAttribute("CrawlerRanker");
		 
		Properties config = engine.getConfig();
		String fileList = null;
		fileList = sr.ssearch(config.getProperty(CrawlerConstants.ES_INDEX_NAME),
				config.getProperty(CrawlerConstants.CRAWLER_TYPE_NAME), query, "and", // please
																						// replace
																						// it
																						// with
																						// and,
																						// or,
																						// phrase
				rr);

		PrintWriter out = response.getWriter();
		out.print(fileList);
		out.flush();
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}
}
