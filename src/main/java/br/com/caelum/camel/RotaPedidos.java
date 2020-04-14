package br.com.caelum.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.dataformat.JsonLibrary;

public class RotaPedidos {

	public static void main(String[] args) throws Exception {

		CamelContext context = new DefaultCamelContext();
		context.addRoutes(new RouteBuilder() {

			@Override
			public void configure() throws Exception {
				
				errorHandler(
					    deadLetterChannel("file:erro").
					        maximumRedeliveries(3).
					            redeliveryDelay(3000).
					        onRedelivery(new Processor() {            
					            @Override
					            public void process(Exchange exchange) throws Exception {
					                        int counter = (int) exchange.getIn().getHeader(Exchange.REDELIVERY_COUNTER);
					                        int max = (int) exchange.getIn().getHeader(Exchange.REDELIVERY_MAX_COUNTER);
					                        System.out.println("Redelivery - " + counter + "/" + max );
					                    }
					        })
					);

				from("file:pedidos?delay=5s&noop=true").
			    routeId("rota-pai").
			    log("${file:name}"). //logando nome do arquivo
			    routeId("rota-pedidos").
			    delay(1000). //esperando 1 segundo
			    to("validator:pedido.xsd");
				
//			    multicast().
//			    parallelProcessing().
//			    to("direct:http").
//			    to("direct:soap");
				
				from("direct:soap").
				routeId("rota-soap").
				to("xslt:pedido-para-soap.xslt").
				log("${body}").
				setHeader(Exchange.CONTENT_TYPE, constant("text/xml")).
				to("mock:soap");
				
				from("direct:http").
				routeId("rota-http").
				setProperty("pedidoId", xpath("/pedido/id/text()")).
			    setProperty("clienteId", xpath("/pedido/pagamento/email-titular/text()")).
			    split().
			        xpath("/pedido/itens/item").
			    filter().
			        xpath("/item/formato[text()='EBOOK']").
			    setProperty("ebookId", xpath("/item/livro/codigo/text()")).
			    log("${id} \n ${body}").
			    marshal().
			    json(JsonLibrary.Jackson).	    
			    setHeader(Exchange.HTTP_QUERY, 
			            simple("clienteId=${property.clienteId}&pedidoId=${property.pedidoId}&ebookId=${property.ebookId}")).
			to("http4://localhost:8080/webservices/ebook/item");
			}
			
		});

		context.start();
		Thread.sleep(20000);
		context.stop();
	}	
}
