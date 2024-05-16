package br.com.brunocs;

import br.com.brunocs.kafka.KafkaDispatch;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class GenerateAllReportsServlet extends HttpServlet {
    private final KafkaDispatch batchDispatch = new KafkaDispatch<String>();

    @Override
    public void destroy() {
        super.destroy();
        batchDispatch.close();
    }


    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {


        try {
            batchDispatch.send("SEND_MESSAGE_TO_ALL_USERS", "USER_GENERATE_READING_REPORTER", "USER_GENERATE_READING_REPORTER");

            System.out.println("Sent generate reports to all Users");
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("Report requests generated");
        } catch (ExecutionException | InterruptedException e) {
            throw new ServletException(e);
        }
    }
}



