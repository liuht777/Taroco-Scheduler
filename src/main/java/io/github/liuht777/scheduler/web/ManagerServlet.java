package io.github.liuht777.scheduler.web;

import io.github.liuht777.scheduler.TaskHelper;
import io.github.liuht777.scheduler.core.Task;
import org.apache.commons.lang3.StringUtils;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static io.github.liuht777.scheduler.constant.DefaultConstants.STATUS_RUNNING;
import static io.github.liuht777.scheduler.constant.DefaultConstants.STATUS_STOP;
import static io.github.liuht777.scheduler.constant.DefaultConstants.TYPE_TAROCO_TASK;


/**
 * @author liuht
 */
@WebServlet(name = "scheduler", urlPatterns = "/taroco/scheduler")
public class ManagerServlet extends HttpServlet {

    private static final long serialVersionUID = 8160082230341182715L;

    private static final String HEAD =
            "<!DOCTYPE html>\n" +
                    "<html>\n" +
                    "<head>\n" +
                    "<meta charset=\"utf-8\"/>\n" +
                    "\t  <title>taroco-Schedule管理</title>\n" +
                    "\t  <meta http-equiv=\"X-UA-Compatible\" content=\"IE=edge\">\n" +
                    "\t  <meta name=\"viewport\" content=\"width=device-width\"/>\n" +
                    "\t  <meta name=\"keywords\" content=\"taroco,liuht\"/>\n" +
                    "\t  <meta name=\"description\" content=\"taroco-Scheduler管理\"/>\n" +
                    "\t  <link rel=\"stylesheet\"  href=\"http://cdn.bootcss.com/bootstrap/3.3.4/css/bootstrap.min.css\">\n" +
                    "\t  <script type=\"text/javascript\" src=\"http://apps.bdimg.com/libs/jquery/2.1.4/jquery.min.js\"></script>\n" +
                    "\t  <script type=\"text/javascript\" src=\"http://cdn.bootcss.com/bootstrap/3.3.4/js/bootstrap.min.js\"></script>\n" +
                    "</head>\n";

    private static final String SCRIPT =
            "\t	<script type=\"text/javascript\">\n" +
                    "\t		$(document).ready(function(){\n" +
                    "\t			$(\"#myModal\").on('show.bs.modal', function(event){\n" +
                    "\t		    var button = $(event.relatedTarget); \n" +
                    "\t			var titleData = button.data('title'); \n" +
                    "\t		    var modal = $(this)\n" +
                    "\t	       	modal.find('.modal-title').text(titleData + '定时任务');\n" +
                    "\t	  		});\n" +
                    "\t		});\n" +
                    "\t		function formSubmit(){\n" +
                    "\t			document.getElementById(\"addform\").submit();\n" +
                    "\t		}\n" +
                    "\t	</script>";


    private static final String PAGE =
            "\t <body>\n" +
                    "\t <div class=\"container-fluid\">\n" +
                    "\t 	<h1>taroco-Scheduler管理页面</h1>\n" +
                    "\t     <a  target=\"_blank\" href=\"https://github.com/liuht777/taroco-scheduler\">【taroco-scheduler】</a>\t\t" +
                    "\t     <div class=\"navbar-right\">\n" +
                    "\t     	<button type=\"button\" class=\"btn btn-primary\"  data-toggle=\"modal\" data-target=\"#myModal\" data-title=\"新增\">新增</button>\n" +
                    "\t     </div>\n" +
                    "\t     <div id=\"myModal\" class=\"modal fade\">\n" +
                    "\t         <div class=\"modal-dialog\">\n" +
                    "\t             <div class=\"modal-content\">\n" +
                    "\t                 <div class=\"modal-header\">\n" +
                    "\t                     <button type=\"button\" class=\"close\" data-dismiss=\"modal\" aria-hidden=\"true\">&times;</button>\n" +
                    "\t                     <h4 class=\"modal-title\">Modal Window</h4>\n" +
                    "\t                 </div>\n" +
                    "\t                 <div class=\"modal-body\">\n" +
                    "\t 					<div class=\"container\">\n" +
                    "\t 						<form id=\"addform\" method=\"post\" action=\"%s\" class=\"form-horizontal\">\n" +
                    "\t 						<div class=\"row\">\n" +
                    "\t 							<div class=\"col-md-6\">\n" +
                    "\t 									<div class=\"form-group\">\n" +
                    "\t 										<label class=\"col-sm-4 control-label\" for=\"bean\">bean名称<span style=\"color:red\">*</span></label>\n" +
                    "\t 										<div class=\"col-sm-6\">\n" +
                    "\t 											<input id=\"bean\" name=\"bean\" type=\"text\" class=\"form-control\" required>\n" +
                    "\t 										</div>\n" +
                    "\t 									</div>\n" +
                    "\t 									<div class=\"form-group\">\n" +
                    "\t 										<label class=\"col-sm-4 control-label\" for=\"method\">方法名称<span style=\"color:red\">*</span></label>\n" +
                    "\t 										<div class=\"col-sm-6\">\n" +
                    "\t 											<input id=\"method\" name=\"method\" type=\"text\" class=\"form-control\" required>\n" +
                    "\t 										</div>\n" +
                    "\t 									</div>\n" +
                    "\t 									<div class=\"form-group\">\n" +
                    "\t 										<label class=\"col-sm-4 control-label\" for=\"cronExpression\">corn表达式</label>\n" +
                    "\t 										<div class=\"col-sm-6\">\n" +
                    "\t 											<input id=\"cronExpression\" name=\"cronExpression\" type=\"text\" class=\"form-control\" required>\n" +
                    "\t 										</div>\n" +
                    "\t 									</div>\n" +
                    "\t 									<div class=\"form-group\">\n" +
                    "\t 										<label class=\"col-sm-4 control-label\" for=\"period\">周期（毫秒）</label>\n" +
                    "\t 										<div class=\"col-sm-6\">\n" +
                    "\t 											<input id=\"period\" name=\"period\" type=\"text\" class=\"form-control\" required>\n" +
                    "\t 										</div>\n" +
                    "\t 									</div>\n" +
                    "\t 									<div class=\"form-group\">\n" +
                    "\t 										<label class=\"col-sm-4 control-label\" for=\"startTime\">开始时间</label>\n" +
                    "\t 										<div class=\"col-sm-6\">\n" +
                    "\t 											<input id=\"startTime\" name=\"startTime\" type=\"text\" class=\"form-control\" placeholder=\"yyyy-MM-dd HH:mm:ss SSS\">\n" +
                    "\t 										</div>\n" +
                    "\t 									</div>\n" +
                    "\t 									<div class=\"form-group\">\n" +
                    "\t 										<label class=\"col-sm-4 control-label\" for=\"param\">参数(字符串)</label>\n" +
                    "\t 										<div class=\"col-sm-6\">\n" +
                    "\t 											<input id=\"param\" name=\"param\" type=\"text\" class=\"form-control\" required>\n" +
                    "\t 										</div>\n" +
                    "\t 									</div>\n" +
                    "\t 									<div class=\"form-group\">\n" +
                    "\t 										<label class=\"col-sm-4 control-label\" for=\"param\">后缀</label>\n" +
                    "\t 										<div class=\"col-sm-6\">\n" +
                    "\t 											<input id=\"extKeySuffix\" name=\"extKeySuffix\" type=\"text\" class=\"form-control\" required>\n" +
                    "\t 										</div>\n" +
                    "\t 									</div>\n" +
                    "\t              		   				<div class=\"modal-footer\">\n" +
                    "\t               		      				<button type=\"button\" class=\"btn btn-default\" data-dismiss=\"modal\">取消</button>\n" +
                    "\t               		      				<button type=\"button\" onclick=\"formSubmit()\" class=\"btn btn-primary\">保存</button>\n" +
                    "\t             	    				</div>\n" +
                    "\t                 				</div>\n" +
                    "\t                 			</div>\n" +

                    "\t 							</form>\n" +
                    "\t         				</div>\n" +
                    "\t         			</div>\n" +
                    "\t         		</div>\n" +
                    "\t     	</div>\n" +
                    "\t 	</div>\n" +
                    "\t </div>\n" +
                    "\t <div class=\"container-fluid\">\n" +
                    "\t 	<div class=\"row-fluid\">\n" +
                    "\t 		<div class=\"span12\">\n" +
                    "\t 			<h3>集群节点</h3>\n" +
                    "\t 			<table class=\"table\">\n" +
                    "\t 				<thead>\n" +
                    "\t 					<tr>\n" +
                    "\t 						<th width=\"100px\">序号</th>\n" +
                    "\t 						<th>名称</th>\n" +
                    "\t 						<th>调度节点</th>\n" +
                    "\t 					</tr>\n" +
                    "\t 				</thead>\n" +
                    "\t 				<tbody>\n" +
                    "\t 					%s \n" +
                    "\t 				</tbody>\n" +
                    "\t 			</table>\n" +
                    "\t 		</div>\n" +
                    "\t 		<div class=\"span12\">\n" +
                    "\t 			<h3>定时任务列表</h3>\n" +
                    "\t 			<table class=\"table\">\n" +
                    "\t 				<thead>\n" +
                    "\t 					<tr>\n" +
                    "\t 						<th width=\"50\">序号</th>\n" +
                    "\t 						<th width=\"50\">目标bean</th>\n" +
                    "\t 						<th width=\"50\">目标方法</th>\n" +
                    "\t 						<th width=\"50\">参数</th>\n" +
                    "\t 						<th width=\"50\">类型</th>\n" +
                    "\t 						<th width=\"50\">cron表达式</th>\n" +
                    "\t 						<th width=\"50\">开始时间</th>\n" +
                    "\t 						<th width=\"50\">周期（毫秒）</th>\n" +
                    "\t 						<th width=\"50\">执行节点</th>\n" +
                    "\t 						<th width=\"50\">运行状态</th>\n" +
                    "\t 						<th width=\"50\">执行次数</th>\n" +
                    "\t 						<th width=\"50\">最近执行时间</th>\n" +
                    "\t 						<th width=\"50\">操作</th>\n" +
                    "\t 					</tr>\n" +
                    "\t 				</thead>\n" +
                    "\t 				<tbody>\n" +
                    "\t 					%s\n " +
                    "\t 				</tbody>\n" +
                    "\t 			</table>\n" +
                    "\t 		</div>\n" +
                    "\t 	</div>\n" +
                    "\t </div>\n" +
                    "\t </body>";


    @Override
    protected void service(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String del = request.getParameter("del");
        String start = request.getParameter("start");
        String stop = request.getParameter("stop");
        String bean = request.getParameter("bean");
        String method = request.getParameter("method");
        if (StringUtils.isNotEmpty(del)) {
            Task task = new Task();
            String[] dels = del.split("_");
            task.setTargetBean(dels[0]);
            task.setTargetMethod(dels[1]);
            if (dels.length > 2) {
                task.setExtKeySuffix(dels[2]);
            }
            TaskHelper.delScheduleTask(task);
            response.sendRedirect(request.getSession().getServletContext().getContextPath() + "/taroco/scheduler");
        } else if (StringUtils.isNotEmpty(start)) {
            Task task = new Task();
            String[] dels = start.split("_");
            task.setTargetBean(dels[0]);
            task.setTargetMethod(dels[1]);
            if (dels.length > 2) {
                task.setExtKeySuffix(dels[2]);
            }
            task.setStatus(STATUS_RUNNING);
            TaskHelper.updateScheduleTask(task);
            response.sendRedirect(request.getSession().getServletContext().getContextPath() + "/taroco/scheduler");
        } else if (StringUtils.isNotEmpty(stop)) {
            Task task = new Task();
            String[] dels = stop.split("_");
            task.setTargetBean(dels[0]);
            task.setTargetMethod(dels[1]);
            if (dels.length > 2) {
                task.setExtKeySuffix(dels[2]);
            }
            task.setStatus(STATUS_STOP);
            TaskHelper.updateScheduleTask(task);
            response.sendRedirect(request.getSession().getServletContext().getContextPath() + "/taroco/scheduler");
        } else if (StringUtils.isNotEmpty(bean) && StringUtils.isNotEmpty(method)) {
            Task task = new Task();
            task.setTargetBean(bean);
            task.setTargetMethod(method);
            task.setType(TYPE_TAROCO_TASK);
            String cronExpression = request.getParameter("cronExpression");
            if (StringUtils.isNotEmpty(cronExpression)) {
                task.setCronExpression(cronExpression);
            }
            String period = request.getParameter("period");
            if (StringUtils.isNotEmpty(period)) {
                task.setPeriod(Long.valueOf(period));
            }
            String startTime = request.getParameter("startTime");
            if (StringUtils.isNotEmpty(startTime)) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
                Date date = null;
                try {
                    date = sdf.parse(startTime);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                task.setStartTime(date);
            } else {
                task.setStartTime(new Date());
            }
            String param = request.getParameter("param");
            if (StringUtils.isNotEmpty(param)) {
                task.setParams(param);
            }
            String extKeySuffix = request.getParameter("extKeySuffix");
            if (StringUtils.isNotEmpty(extKeySuffix)) {
                task.setExtKeySuffix(extKeySuffix);
            }
            if (StringUtils.isNotEmpty(cronExpression) || StringUtils.isNotEmpty(period) || StringUtils.isNotEmpty(startTime)) {
                TaskHelper.addScheduleTask(task);
            }
            response.sendRedirect(request.getSession().getServletContext().getContextPath() + "/taroco/scheduler");
        }
        try {
            List<String> servers = TaskHelper.getZkClient().getiSchedulerServer().loadScheduleServerNames();
            if (servers != null) {
                response.setContentType("text/html;charset=UTF-8");
                PrintWriter out = response.getWriter();
                StringBuffer sb = new StringBuffer();
                for (int i = 0; i < servers.size(); i++) {
                    String ser = servers.get(i);
                    sb.append("<tr class=\"info\">")
                            .append("<td>").append(i + 1).append("</td>")
                            .append("<td>").append(ser).append("</td>");
                    if (TaskHelper.getZkClient().getiSchedulerServer().isLeader(ser, servers)) {
                        sb.append("<td>").append("是").append("</td>");
                    } else {
                        sb.append("<td>").append("否").append("</td>");
                    }
                    sb.append("</tr>");
                }

                List<Task> tasks = TaskHelper.queryScheduleTask();
                StringBuilder sbTask = new StringBuilder();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                for (int i = 0; i < tasks.size(); i++) {
                    Task task = tasks.get(i);
                    sbTask.append("<tr class=\"info\">")
                            .append("<td>").append(i + 1).append("</td>")
                            .append("<td>").append(task.getTargetBean()).append("</td>")
                            .append("<td>").append(task.getTargetMethod4Show()).append("</td>")
                            .append("<td>").append(task.getParams()).append("</td>")
                            .append("<td>").append(task.getType()).append("</td>")
                            .append("<td>").append(task.getCronExpression()).append("</td>")
                            .append("<td>").append(task.getStartTime()).append("</td>")
                            .append("<td>").append(task.getPeriod()).append("</td>")
                            .append("<td>").append(task.getCurrentServer()).append("</td>")
                            .append("<td>").append(task.getStatus()).append("</td>")
                            .append("<td>").append(task.getRunTimes()).append("</td>");
                    if (task.getLastRunningTime() > 0) {
                        Date date = new Date(task.getLastRunningTime());
                        sbTask.append("<td>").append(sdf.format(date)).append("</td>");
                    } else {
                        sbTask.append("<td>").append("-").append("</td>");
                    }
                    sbTask.append("<td>");
                    if (task.isStop()) {
                        sbTask.append("<a href=\"").append(request.getSession().getServletContext().getContextPath())
                                .append("/taroco/scheduler?start=")
                                .append(task.getTargetBean())
                                .append("_")
                                .append(task.getTargetMethod());
                        if (StringUtils.isNotBlank(task.getExtKeySuffix())) {
                            sbTask.append("_").append(task.getExtKeySuffix());
                        }
                        sbTask.append("\" style=\"color:green\">运行</a>");
                    } else {
                        sbTask.append("<a href=\"").append(request.getSession().getServletContext().getContextPath())
                                .append("/taroco/scheduler?stop=")
                                .append(task.getTargetBean())
                                .append("_")
                                .append(task.getTargetMethod());
                        if (StringUtils.isNotBlank(task.getExtKeySuffix())) {
                            sbTask.append("_").append(task.getExtKeySuffix());
                        }
                        sbTask.append("\" style=\"color:red\">停止</a>");
                    }
                    sbTask.append(" <a href=\"").append(request.getSession().getServletContext().getContextPath())
                            .append("/taroco/scheduler?del=")
                            .append(task.getTargetBean())
                            .append("_")
                            .append(task.getTargetMethod());
                    if (StringUtils.isNotBlank(task.getExtKeySuffix())) {
                        sbTask.append("_").append(task.getExtKeySuffix());
                    }
                    sbTask.append("\" >删除</a>")
                            .append("</td>");
                    sbTask.append("</tr>");
                }
                out.write(HEAD);
                out.write(SCRIPT);
                out.write(String.format(PAGE, request.getSession().getServletContext().getContextPath() + "/taroco/scheduler",
                        sb.toString(), sbTask.toString()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
