package net.elodina.mesos.hdfs;

import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpServer {
    private static final Logger logger = Logger.getLogger(HttpServer.class);

    private Server server;

    public void start() throws Exception {
        if (server != null) throw new IllegalStateException("started");
        Scheduler.Config config = Scheduler.$.config;

        QueuedThreadPool threadPool = new QueuedThreadPool(Runtime.getRuntime().availableProcessors() * 16);
        threadPool.setName("Jetty");

        server = new Server(threadPool);
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(config.apiPort());
        connector.setIdleTimeout(60 * 1000);

        ServletContextHandler handler = new ServletContextHandler();
        handler.addServlet(new ServletHolder(new Servlet()), "/");
        handler.setErrorHandler(new ErrorHandler());

        server.setHandler(handler);
        server.addConnector(connector);
        server.start();

        logger.info("started on port " + connector.getLocalPort());
    }

    public void stop() throws Exception {
        if (server == null) throw new IllegalStateException("!started");

        server.stop();
        server.join();
        server = null;

        logger.info("stopped");
    }

    private class Servlet extends HttpServlet {
        protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException { doGet(request, response); }

        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
            String url = request.getRequestURL() + (request.getQueryString() != null ? "?" + request.getQueryString() : "");
            logger.info("handling - " + url);

            try {
                handle(request, response);
                logger.info("finished handling");
            } catch (HttpError e) {
                response.sendError(e.getCode(), e.getMessage());
            } catch (Exception e) {
                logger.error("error handling", e);
                response.sendError(500, "" + e);
            }
        }

        private void handle(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
            String uri = request.getRequestURI();

            if (uri.equals("/health")) handleHealth(response);
            else if (uri.startsWith("/api/node")) handleNodeApi(request, response);
            else if (uri.startsWith("/jar/")) downloadFile(Scheduler.$.config.jar, response);
            else throw new HttpError(404, "not found");
        }

        private void handleHealth(HttpServletResponse response) throws IOException {
            response.setContentType("text/plain; charset=utf-8");
            response.getWriter().println("ok");
        }

        private void handleNodeApi(HttpServletRequest request, HttpServletResponse response) throws IOException {
            String uri = request.getRequestURI();
            uri = uri.substring("/api/node".length());

            request.setAttribute("jsonResponse", true);
            response.setContentType("application/json; charset=utf-8");

            switch (uri) {
                case "/list": handleNodeList(response); break;
                case "/add": case "/update": handleNodeAddUpdate(request, response, uri.equals("/add")); break;
                default: throw new HttpError(404, "unsupported method " + uri);
            }
        }

        private void handleNodeList(HttpServletResponse response) throws IOException {
            List<Node> nodes = Nodes.$.getNodes();

            @SuppressWarnings("unchecked") List<JSONObject> nodesJson = new JSONArray();
            for (Node node : nodes) nodesJson.add(node.toJson());

            response.getWriter().println("" + nodesJson);
        }

        private void handleNodeAddUpdate(HttpServletRequest request, HttpServletResponse response, boolean add) throws IOException {
            String id = request.getParameter("node");
            if (id == null || id.isEmpty()) throw new HttpError(400, "node required");

            if (add && Nodes.$.getNode(id) != null) throw new HttpError(400, "duplicate node");
            if (!add && Nodes.$.getNode(id) == null) throw new HttpError(400, "node not found");

            Double cpus = null;
            if (request.getParameter("cpus") != null)
                try { cpus = Double.valueOf(request.getParameter("cpus")); }
                catch (IllegalArgumentException e) { throw new HttpError(400, "invalid cpus"); }

            Long mem = null;
            if (request.getParameter("mem") != null)
                try { mem = Long.valueOf(request.getParameter("mem")); }
                catch (IllegalArgumentException e) { throw new HttpError(400, "invalid mem"); }

            Node node;
            if (add) node = Nodes.$.addNode(new Node(id));
            else node = Nodes.$.getNode(id);

            if (cpus != null) node.cpus = cpus;
            if (mem != null) node.mem = mem;

            Nodes.$.save();

            @SuppressWarnings("unchecked") List<JSONObject> nodesJson = new JSONArray();
            nodesJson.add(node.toJson());
            response.getWriter().println("" + nodesJson);
        }

        private void downloadFile(File file, HttpServletResponse response) throws IOException {
            response.setContentType("application/zip");
            response.setHeader("Content-Length", "" + file.length());
            response.setHeader("Content-Disposition", "attachment; filename=\"" + file.getName() + "\"");
            Util.IO.copyAndClose(new FileInputStream(file), response.getOutputStream());
        }
    }

    private class ErrorHandler extends org.eclipse.jetty.server.handler.ErrorHandler {
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response_) throws IOException {
            Response response = (Response) response_;
            int code = response.getStatus();

            String error = response.getReason() != null ? response.getReason() : "";
            PrintWriter writer = response.getWriter();

            if (request.getAttribute("jsonResponse") != null) {
                response.setContentType("application/json; charset=utf-8");

                Map<String, Object> map = new HashMap<>();
                map.put("code", code);
                map.put("error", error);

                writer.println("" + new JSONObject(map));
            } else {
                response.setContentType("text/plain; charset=utf-8");
                writer.println(code + " - " + error);
            }

            writer.flush();
            baseRequest.setHandled(true);
        }
    }

    class HttpError extends RuntimeException {
        private int code;

        public HttpError(int code, String message) {
            super(message);
            this.code = code;
        }

        public int getCode() { return code; }
    }
}