package br.com.brunocs;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * Hello world!
 *
 */
public class HttpEcommerceService
{
    public static void main( String[] args ) throws Exception {
        var server = new Server(8080);
        var context = new ServletContextHandler();
        context.setContextPath("/");
        context.addServlet(new ServletHolder(new NewOrderServlet()), "/new-order");
        server.setHandler(context);
        server.start();
        server.join();
    }
}
