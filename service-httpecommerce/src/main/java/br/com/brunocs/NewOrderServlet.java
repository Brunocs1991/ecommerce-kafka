package br.com.brunocs;

import br.com.brunocs.kafka.dispatcher.KafkaDispatch;
import br.com.brunocs.kafka.utils.CorrelationId;
import br.com.brunocs.model.Order;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {
    private final KafkaDispatch orderDispatcher = new KafkaDispatch<Order>();

    @Override
    public void destroy() {
        super.destroy();
        orderDispatcher.close();
    }


    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {


        try {

            var email = req.getParameter("email");
            var amount = new BigDecimal(req.getParameter("amount"));

            var orderId = UUID.randomUUID().toString();
            var order = new Order(orderId, amount, email);
            orderDispatcher.send(
                    "ECOMMERCE_NEW_ORDER",
                    email,
                    new CorrelationId(NewOrderServlet.class.getSimpleName()),
                    order
            );
            System.out.println("New order sent sucessfully");
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("new order sent sucessfully");
        } catch (ExecutionException | InterruptedException e) {
            throw new ServletException(e);
        }
    }
}



