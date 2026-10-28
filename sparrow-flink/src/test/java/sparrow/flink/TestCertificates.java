package sparrow.flink;

import java.io.File;
import java.io.OutputStream;
import java.io.Writer;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JcaPKCS8Generator;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/**
 * A self-signed server certificate for {@code localhost}, generated per test run: PEM certificate and PKCS#8
 * key for the server, a PKCS12 truststore with the certificate for the client.
 */
final class TestCertificates {

    /** Password of {@link #truststore}. */
    static final String TRUSTSTORE_PASSWORD = "sparrow-test";

    final File certificate;
    final File privateKey;
    final File truststore;

    private TestCertificates(File certificate, File privateKey, File truststore) {
        this.certificate = certificate;
        this.privateKey = privateKey;
        this.truststore = truststore;
    }

    /** Generates a certificate for {@code localhost} (subject alternative names localhost and 127.0.0.1). */
    static TestCertificates generate(Path dir) throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair keys = generator.generateKeyPair();
        Instant now = Instant.now();
        X500Name name = new X500Name("CN=localhost");
        JcaX509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(name, BigInteger.valueOf(now.toEpochMilli()),
                Date.from(now.minus(Duration.ofHours(1))), Date.from(now.plus(Duration.ofDays(1))), name,
                keys.getPublic());
        builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
        builder.addExtension(Extension.subjectAlternativeName, false, new GeneralNames(new GeneralName[] {
            new GeneralName(GeneralName.dNSName, "localhost"), new GeneralName(GeneralName.iPAddress, "127.0.0.1")}));
        X509Certificate certificate = new JcaX509CertificateConverter().getCertificate(
                builder.build(new JcaContentSignerBuilder("SHA256withRSA").build(keys.getPrivate())));

        Path cert = dir.resolve("server.crt");
        try (Writer w = Files.newBufferedWriter(cert, StandardCharsets.US_ASCII); JcaPEMWriter pem = new JcaPEMWriter(w)) {
            pem.writeObject(certificate);
        }
        Path key = dir.resolve("server.key");
        try (Writer w = Files.newBufferedWriter(key, StandardCharsets.US_ASCII); JcaPEMWriter pem = new JcaPEMWriter(w)) {
            pem.writeObject(new JcaPKCS8Generator(keys.getPrivate(), null));
        }
        Path store = dir.resolve("truststore.p12");
        KeyStore trust = KeyStore.getInstance("PKCS12");
        trust.load(null, null);
        trust.setCertificateEntry("server", certificate);
        try (OutputStream out = Files.newOutputStream(store)) {
            trust.store(out, TRUSTSTORE_PASSWORD.toCharArray());
        }
        return new TestCertificates(cert.toFile(), key.toFile(), store.toFile());
    }
}
