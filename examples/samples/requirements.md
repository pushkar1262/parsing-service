# Merchant Onboarding Platform

Acme Payments needs a self-service portal so merchants can register, submit KYC
documents, and start accepting card payments without a phone call to support.

## Registration

- The system must let a merchant register with an email address and a password
- Verify the email address before the account becomes active
  - Resend the verification link at most three times per hour
- Reject registrations from sanctioned jurisdictions

## KYC review

Merchants upload identity documents, which a compliance officer reviews manually.
Review decisions shall be recorded with the reviewer's identity and a timestamp.

### Document requirements

| Document | Required | Retention |
| --- | --- | --- |
| Government ID | Yes | 7 years |
| Proof of address | Yes | 7 years |
| Company registration | Business accounts only | 10 years |

## Performance and availability

The portal shall authenticate users within 300ms at p99. Uploads of up to 25MB
must complete without a timeout on a 5Mbps connection.

Make the dashboard fast.

## Settlement

Funds settle to the merchant's nominated bank account on a T+2 schedule. Failed
settlements must be retried automatically and surfaced in the merchant dashboard.
