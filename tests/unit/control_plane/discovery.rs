use super::*;

#[test]
fn discovery_names_the_public_origin_instead_of_the_token_issuer() -> Result<()> {
    let document: serde_json::Value =
        serde_json::from_slice(&openid_configuration("https://actors.example/")?)?;
    assert_eq!(document["issuer"], "https://actors.example");
    assert_eq!(
        document["jwks_uri"],
        "https://actors.example/.well-known/jwks.json"
    );
    Ok(())
}
