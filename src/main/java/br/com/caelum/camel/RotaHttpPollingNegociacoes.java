package br.com.caelum.camel;

import java.text.SimpleDateFormat;

import javax.sql.DataSource;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.dataformat.xstream.XStreamDataFormat;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.SimpleRegistry;
import org.apache.commons.dbcp.BasicDataSource;

import com.thoughtworks.xstream.XStream;

public class RotaHttpPollingNegociacoes {

	public static void main(String[] args) throws Exception {
		
		SimpleRegistry registro = new SimpleRegistry();
		registro.put("ds", setupDataSource());
		CamelContext context = new DefaultCamelContext(registro);
		
		final XStream xstream = new XStream();
		xstream.alias("negociacao", Negociacao.class);

		context.addRoutes(new RouteBuilder() {

			@Override
			public void configure() throws Exception {

				from("timer://negociacoes?fixedRate=true&delay=1s&period=360s")
				.setHeader(Exchange.HTTP_METHOD, constant(org.apache.camel.component.http4.HttpMethods.GET))
				.to("https4://argentumws-spring.herokuapp.com/negociacoes")
				.convertBodyTo(String.class)
				.unmarshal(new XStreamDataFormat(xstream))
				.split(body())
				.log("${body}")
				.process(new Processor() {
			        @Override
			        public void process(Exchange exchange) throws Exception {
			            Negociacao negociacao = exchange.getIn().getBody(Negociacao.class);
			            exchange.setProperty("preco", negociacao.getPreco());
			            exchange.setProperty("quantidade", negociacao.getQuantidade());
			            String data = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss").format(negociacao.getData().getTime());
			            exchange.setProperty("data", data);
			        }
			      })
				.setBody(simple("insert into negociacao(preco, quantidade, data) values (${property.preco}, ${property.quantidade}, '${property.data}')"))
				.log("${body}")
				.delay(3000)
				.to("jdbc:ds")
				.end();
//				.marshal()
//					.xmljson()
//				.log("${id} - ${body}")
//				.setHeader(Exchange.FILE_NAME, simple("${date:now:YYYmmdd}.json"))
//				.log("${header.filename}")
//				.to("file:saida");
				
			}
			
		});

		context.start();
		Thread.sleep(20000);
		context.stop();
	}	

	private static DataSource setupDataSource() {
		BasicDataSource ds = new BasicDataSource();
		ds.setUsername("postgres");
		ds.setDriverClassName("org.postgresql.Driver");
		ds.setPassword("admin");
		ds.setUrl("jdbc:postgresql://localhost:5432/camel");
		return ds;
	}
}
