package net.plumbing.msgbus.controller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
//import org.springframework.web.context.request.async.DeferredResult;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

@RestController
public class OptionController {

    private static final Logger Controller_log = LoggerFactory.getLogger(OptionController.class);
   //@GetMapping(path ="/HermesService/InternalRestApi/**", produces = MediaType.ALL_VALUE,  consumes = MediaType.ALL_VALUE)
 //  @RequestMapping(value="/HermesService/InternalRestApi/apiSQLRequest/**", headers={"Access-Control-Request-Headers=content-type"}  ,method={RequestMethod.OPTIONS},  produces = MediaType.ALL_VALUE,  consumes = MediaType.ALL_VALUE)
   //@RequestMapping(value="/HermesService/InternalRestApi/apiSQLRequest/**" ,method={RequestMethod.OPTIONS},  produces = MediaType.ALL_VALUE,  consumes = MediaType.ALL_VALUE)
 //  @GetMapping(path ={"/HermesService/GetHttpRequest/*", "/HermesSOAPService/GetHttpRequest/*" }, produces = MediaType.ALL_VALUE,  consumes = MediaType.ALL_VALUE)
   @RequestMapping(value={"/HermesService/GetHttpRequest/**", "/MsgBusService/GetHttpRequest/**"} ,method={RequestMethod.OPTIONS} )
    @ResponseStatus(HttpStatus.OK)
    //  @ResponseBody
    @CrossOrigin(origins = "*")
    public void OptionHttpRequest( ServletRequest getServletRequest, HttpServletResponse getResponse) {
        HttpServletRequest httpRequest = (HttpServletRequest) getServletRequest;
        Controller_log.warn("OptionRestApi (RemoteAddr): \"" + getServletRequest.getRemoteAddr() + "\" ,RemoteHost: \"" + getServletRequest.getRemoteHost() + "\"");
        String url = httpRequest.getRequestURL().toString();
        String AccessControlRequestHeaders = httpRequest.getHeader("Access-Control-Request-Headers");
        String queryString;
        try {
            queryString = URLDecoder.decode(httpRequest.getQueryString(), "UTF-8");
        } catch (UnsupportedEncodingException | NullPointerException e) {
            queryString = httpRequest.getQueryString();
        }
        Controller_log.warn("Access-Control-Request-Headers= " + AccessControlRequestHeaders);

        Controller_log.warn("url= (" + url + ") queryString(" + queryString + ")");
        Controller_log.warn("httpRequest.Option.HttpRequest.getMethod()" + httpRequest.getMethod() + ": url= (" + url + ")" + ( (queryString!=null) ?  "queryString(" + queryString + ")" : " "));
//    String HttpResponse = "{ \"Response\": \"" +
//            XML.escape(httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")") +
//            "\"}";
        getResponse.setContentType("text/json;Charset=UTF-8");
        getResponse.setHeader("Access-Control-Allow-Methods","GET,PUT,POST,DELETE,OPTIONS");
        getResponse.setHeader("Allow", "POST,GET,PUT,DELETE,OPTIONS");
        getResponse.setHeader("Access-Control-Allow-Origin","*");
        getResponse.setHeader("Access-Control-Expose-Headers","*, Authorization");
        getResponse.setHeader("Access-Control-Expose-Headers", "AccessControlRequestHeaders");
        getResponse.setHeader("Access-Control-Expose-Headers", "X-Total-Count");
        getResponse.setHeader("Access-Control-Expose-Headers", "Content-Range");
        getResponse.setStatus(200);
        return  ;
    }

    @RequestMapping(value="/HermesService/InternalRestApi/apiSQLRequest/**" ,method={RequestMethod.OPTIONS} )
   @ResponseStatus(HttpStatus.OK)
 //  @ResponseBody
   @CrossOrigin(origins = "*")
public void OptionRestApi( ServletRequest getServletRequest, HttpServletResponse getResponse) {
    HttpServletRequest httpRequest = (HttpServletRequest) getServletRequest;
    Controller_log.warn("OptionRestApi (RemoteAddr): \"" + getServletRequest.getRemoteAddr() + "\" ,RemoteHost: \"" + getServletRequest.getRemoteHost() + "\"");
    String url = httpRequest.getRequestURL().toString();
    String AccessControlRequestHeaders = httpRequest.getHeader("Access-Control-Request-Headers");
    String queryString;
    try {
        queryString = URLDecoder.decode(httpRequest.getQueryString(), "UTF-8");
    } catch (UnsupportedEncodingException | NullPointerException e) {
        queryString = httpRequest.getQueryString();
    }
    Controller_log.warn("Access-Control-Request-Headers= " + AccessControlRequestHeaders);

    Controller_log.warn("url= (" + url + ") queryString(" + queryString + ")");
    Controller_log.warn("httpRequest.OptionRestApi.getMethod()" + httpRequest.getMethod() + ": url= (" + url + ")" + ( (queryString!=null) ?  "queryString(" + queryString + ")" : " "));
//    String HttpResponse = "{ \"Response\": \"" +
//            XML.escape(httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")") +
//            "\"}";
       getResponse.setContentType("text/json;Charset=UTF-8");
       getResponse.setHeader("Access-Control-Allow-Methods","GET,PUT,POST,DELETE,OPTIONS");
       getResponse.setHeader("Allow", "POST,GET,PUT,DELETE,OPTIONS");
       getResponse.setHeader("Access-Control-Allow-Origin","*");
       getResponse.setHeader("Access-Control-Expose-Headers","*, Authorization");
       getResponse.setHeader("Access-Control-Expose-Headers", "AccessControlRequestHeaders");
       getResponse.setHeader("Access-Control-Expose-Headers", "X-Total-Count");
       getResponse.setHeader("Access-Control-Expose-Headers", "Content-Range");
       getResponse.setStatus(200);
return  ;
}

    @RequestMapping(value="/HermesService/InternalRestApi/apiSQLRequest/**" ,headers = {"Access-Control-Request-Method=PUT"}, method={RequestMethod.OPTIONS},  produces = MediaType.ALL_VALUE,  consumes = MediaType.ALL_VALUE)
    public void OptionPutApi( ServletRequest getServletRequest, HttpServletResponse getResponse) {
        HttpServletRequest httpRequest = (HttpServletRequest) getServletRequest;
        Controller_log.warn("OptionPutApi (RemoteAddr): \"" + getServletRequest.getRemoteAddr() + "\" ,RemoteHost: \"" + getServletRequest.getRemoteHost() + "\"");
        String url = httpRequest.getRequestURL().toString();
        String AccessControlRequestHeaders = httpRequest.getHeader("Access-Control-Request-Headers");
        String queryString;
        try {
            queryString = URLDecoder.decode(httpRequest.getQueryString(), "UTF-8");
        } catch (UnsupportedEncodingException | NullPointerException e) {
            queryString = httpRequest.getQueryString();
        }
        Controller_log.warn("Access-Control-Request-Headers= " + AccessControlRequestHeaders);

        Controller_log.warn("url= (" + url + ") queryString(" + queryString + ")");
        Controller_log.warn("httpRequest.OptionPutApi.Method()" + httpRequest.getMethod() + ": url= (" + url + ")" + ( (queryString!=null) ?  "queryString(" + queryString + ")" : " "));
//    String HttpResponse = "{ \"Response\": \"" +
//            XML.escape(httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")") +
//            "\"}";
        getResponse.setContentType("text/json;Charset=UTF-8");
        getResponse.setHeader("Access-Control-Allow-Methods","GET,PUT,POST,DELETE,OPTIONS");
        getResponse.setHeader("Allow", "POST,GET,PUT,DELETE,OPTIONS");
        getResponse.setHeader("Access-Control-Allow-Origin","*");
        getResponse.setHeader("Access-Control-Expose-Headers","*, Authorization");
        getResponse.setHeader("Access-Control-Expose-Headers", "AccessControlRequestHeaders");
        getResponse.setHeader("Access-Control-Expose-Headers", "X-Total-Count");
        getResponse.setHeader("Access-Control-Expose-Headers", "Content-Range");
        getResponse.setStatus(200);
        return  ;
    }

}
