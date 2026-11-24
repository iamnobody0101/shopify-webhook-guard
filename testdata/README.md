# Test fixtures

`shopify_orders_create_2026-10.json` is a real webhook body generated and signed by Shopify, captured on 2026-11-24 with:

```
shopify app webhook trigger --topic orders/create --api-version 2026-10 \
  --delivery-method http --client-secret local-test-secret --address http://localhost:9099/webhooks
```

It is Shopify's standard sample order (fake data, `@example.com` emails). Shopify sent these headers, which the tests use:

| Header | Value |
|---|---|
| `X-Shopify-Hmac-Sha256` | `/WGG5GIepPtQJVZF3fdeUBzDrzhd0QmX9J+aERuu5mc=` (secret `local-test-secret`) |
| `X-Shopify-Shop-Domain` | `shop.myshopify.com` |
| `X-Shopify-Topic` | `orders/create` |
| `X-Shopify-Webhook-Id` | `a1cf3871-d038-4e02-9717-4ee98d8d21ae` |
| `X-Shopify-Triggered-At` | `2026-11-24T22:09:32.655934808Z` |
| `X-Shopify-Api-Version` | `2026-10` |

The file must stay **byte-exact**: any change, including line endings, invalidates the signature. `.gitattributes` disables line-ending conversion for this folder.
