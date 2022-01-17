package com.bndp.latam.t24.nofiles;

import java.util.Arrays;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import com.temenos.t24.api.complex.eb.enquiryhook.EnquiryContext;
import com.temenos.t24.api.complex.eb.enquiryhook.FilterCriteria;
import com.temenos.t24.api.hook.system.Enquiry;

/**
 * Transact (T24) Enquiry Routine - Consume JMS message from given LOCAL queue and show in ENQUIRY
 * Use JavaExtensilibiltyFramework Interface.
 * 2 values are returned (* separated)
 * - 1 JMSMessageID
 * - 2 MessageBody
 *
 * <code>
 *     ENQUIRY........... BNPE.ENQ.JMS.BROWSER
 *   1 PAGE.SIZE ........ 4,99
 *   2 FILE.NA NOFILE.BNPE.ENQ.JMS.BROWSER
 *   3. 1 FIXE ENQ.ROUTINE
 *   6. 1 SELECTION.FLDS. JMS.CONNECTION
 *   7. 1. 1 GB SEL.LABEL Connection
 *   8. 1 SEL.FLD.OPER... EQ
 *   9. 1 REQUIRED.SEL... Y
 *   6. 2 SELECTION.FLDS. JMS.QUEUE
 *   7. 2. 1 GB SEL.LABEL Queue Name
 *   8. 2 SEL.FLD.OPER... EQ
 *  14. 1 FIELD.NAME..... @ID
 *  15. 1. 1 O 0
 *  16. 1 COLUMN......... 1
 * </code>
 * 
 * @author hpasquel
 *
 */
public class JMSBrowser extends Enquiry {
    
    
    private String SEP = "*";
    
    @Override
    public List<String> setIds(List<FilterCriteria> filterCriteria, EnquiryContext enquiryContext){
        List<String> data = new LinkedList<String>();
        Optional<FilterCriteria> selField;
        List<String> row;
                 
        selField= filterCriteria.stream().filter(p -> p.getFieldname().equals("JMS.QUEUE")).findAny();
        if(selField == null || selField.get() == null || selField.get().getValue().isEmpty()){
            data.add("JMS.QUEUE selection field is mandatory");
            return data;
        }
        String queueName = selField.get().getValue();

        selField= filterCriteria.stream().filter(p -> p.getFieldname().equals("JMS.CONNECTION")).findAny();
        if(selField == null || selField.get() == null || selField.get().getValue().isEmpty()){
            data.add("JMS.CONNECTION selection field is mandatory");
            return data;
        }
        String connection = selField.get().getValue();

        try {
            InitialContext ctx = new InitialContext();
            // lookup the queue object
            Queue queue = (Queue) ctx.lookup("java:/" + queueName);

            // lookup the queue connection factory
            QueueConnectionFactory connFactory = (QueueConnectionFactory) ctx.
                    lookup("java:/" + connection);
            
            // create a queue connection
            QueueConnection queueConn = connFactory.createQueueConnection();
            
            QueueBrowser browser = queueConn.createSession().createBrowser(queue);
            
            Enumeration msgs = browser.getEnumeration();

            while (msgs.hasMoreElements()) { 
                Message tempMsg = (Message)msgs.nextElement(); 
                System.out.println("Message: " + tempMsg); 
                row = Arrays.asList(tempMsg.getJMSMessageID(), tempMsg.getBody(String.class));
                data.add(String.join(SEP, row));                
            }
            
            browser.close();
            queueConn.close();
            
        } catch (NamingException | JMSException e) {
            e.printStackTrace();            
            data.add(e.getMessage());
            return data;
        }

        
        return data;
    }
    
    

}
