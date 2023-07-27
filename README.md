# ETL-off-SQS-queue

### Instructions:
#### All the commands below are supposed to be run in the terminal.

**Step1: Connect to the data and Postgres**

Command: ```docker-compose up```

If you want to restart the environment, simply just put down using the command ```docker-compose down``` and then start again.

**Step2: Install all the needed packages**

Command: ```conda env create -f output_file.yaml```

**Step3: Run the script**

Command: ```python fetch_de.py```

### Questions:
**1. How would you deploy this application in production?**

We can choose a cloud provider such as AWS or managed Kubernetes service to deploy it. Then set up a production-grade Postgres database and SQS queue. Then containerize the Python app using Docker and push the image to a registry. We also need to do version control for code and data. And then automate CI/CD pipeline for deployments.

**2. What other components would you want to add to make this production ready?**

- Use a robust secret management system to securely store and manage sensitive information like database passwords, API keys, and other credentials.
- Set up automated backups for both application data and the database. Implement a disaster recovery plan to quickly recover from catastrophic failures.
- Implement continuous performance testing to ensure the application meets performance expectations under different conditions.

**3.  How can this application scale with a growing dataset?**

- We can add more database serves and distribute the data across them.
- Implement caching mechanisms (e.g., Redis, Memcached) to reduce database load for frequently accessed data
- If applicable, consider partitioning the database table to split it into smaller, more manageable parts

**4.  How can PII be recovered later on?**

I used an encryption-based approach so that it can be recovered later on. You can use below function to recover the masked device_id and IP:

   ```def recover_pii(encrypted_identifier):
    # Create an instance of the Fernet cipher with the secret key
    cipher = Fernet(secret_key)

    # Decrypt the encrypted identifier to recover the original PII
    decrypted_identifier = cipher.decrypt(encrypted_identifier).decode()

    # Split the identifier back into device_id and ip
    device_id, ip = decrypted_identifier.split('_')

    return device_id, ip
```

If you don't want it ever to be recovered, you can use a one-way hash function (SHA-256) to pseudonymize the PII.

**5. What are the assumptions you made?**

I assume that both the SQS queue and the Postgres table are already available and correctly configured. I also assume that the masking logic for device_id and ip is implemented separately. I also assume that the data in the SQS queue is properly formatted and doesn't require extensive validation or sanitization. I deleted the rows with the majority empty values, so I also assumed the missing values are not important.
