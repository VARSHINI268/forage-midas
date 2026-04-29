package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TransactionProcessor {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;

    public TransactionProcessor(UserRepository userRepository, TransactionRecordRepository transactionRecordRepository) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
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
            return; // invalid sender
        }

        // Validate recipient
        UserRecord recipient = userRepository.findById(recipientId);
        if (recipient == null) {
            return; // invalid recipient
        }

        // Check balance
        if (sender.getBalance() < amount) {
            return; // insufficient balance
        }

        // Valid, process transaction
        TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, amount);
        transactionRecordRepository.save(transactionRecord);

        // Update balances
        sender.setBalance(sender.getBalance() - amount);
        recipient.setBalance(recipient.getBalance() + amount);

        userRepository.save(sender);
        userRepository.save(recipient);
    }
}