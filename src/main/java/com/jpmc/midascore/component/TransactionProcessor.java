package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

@Service
public class TransactionProcessor {

    private static final Logger logger = LoggerFactory.getLogger(TransactionProcessor.class);

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;
    private final RestTemplate restTemplate;
    private final String incentiveApiUrl;

    public TransactionProcessor(
            UserRepository userRepository,
            TransactionRecordRepository transactionRecordRepository,
            RestTemplate restTemplate,
            @Value("${incentive.api.url:http://localhost:8080/incentive}") String incentiveApiUrl) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
        this.restTemplate = restTemplate;
        this.incentiveApiUrl = incentiveApiUrl;
    }

    @KafkaListener(topics = "trader-updates", groupId = "midas-core")
    @Transactional
    public void processTransaction(Transaction transaction) {
        long senderId = transaction.getSenderId();
        long recipientId = transaction.getRecipientId();
        float amount = transaction.getAmount();

        // Validate sender
        UserRecord sender = userRepository.findById(senderId);
        if (sender == null) {
            logger.warn("Skipping transaction because sender {} was not found", senderId);
            return; // invalid sender
        }

        // Validate recipient
        UserRecord recipient = userRepository.findById(recipientId);
        if (recipient == null) {
            logger.warn("Skipping transaction because recipient {} was not found", recipientId);
            return; // invalid recipient
        }

        // Check balance
        if (sender.getBalance() < amount) {
            logger.warn("Skipping transaction because sender {} has insufficient balance", senderId);
            return; // insufficient balance
        }

        float incentiveAmount = fetchIncentiveAmount(transaction);
        TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, amount, incentiveAmount);
        transactionRecordRepository.save(transactionRecord);

        // Update balances
        sender.setBalance(sender.getBalance() - amount);
        recipient.setBalance(recipient.getBalance() + amount);

        userRepository.save(sender);
        userRepository.save(recipient);

        logger.info("Processed transaction sender={} recipient={} amount={} incentive={}",
                sender.getName(), recipient.getName(), amount, incentiveAmount);
    }

    private float fetchIncentiveAmount(Transaction transaction) {
        try {
            ResponseEntity<Incentive> response = restTemplate.postForEntity(incentiveApiUrl, transaction, Incentive.class);
            if (response.getBody() != null) {
                float incentiveAmount = response.getBody().getAmount();
                return incentiveAmount >= 0 ? incentiveAmount : 0f;
            }
        } catch (Exception ex) {
            logger.warn("Unable to fetch incentive for transaction {}: {}", transaction, ex.getMessage());
        }
        return 0f;
    }
}
