package pro.surpin.data.arrowflight.server;

import java.time.Clock;

import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * The server's own Flight authenticators for the connector tests (they are package-private in the server):
 * the bearer-token authenticator and the Kerberos (SPNEGO) one that issues signed cluster sessions.
 */
public final class ServerAuthenticators {

    /** Not instantiable. */
    private ServerAuthenticators() {
    }

    /**
     * The server's static bearer-token authenticator ({@code FLIGHT_BEARER_TOKEN}).
     *
     * @param token the token clients must send
     * @return the authenticator
     */
    public static CallHeaderAuthenticator bearer(String token) {
        return FlightSecurity.bearerAuthenticator(token);
    }

    /**
     * The server's Kerberos authenticator, accepting tickets with the service credentials of {@code service}
     * and signing sessions with {@code sessionSecret} (shared by the nodes of a cluster).
     *
     * @param service logged-in service principal (from its keytab)
     * @param sessionSecret HMAC key of the session tokens
     * @return the authenticator
     */
    public static CallHeaderAuthenticator kerberos(UserGroupInformation service, byte[] sessionSecret) {
        return new KerberosFlightAuthenticator(KerberosFlightAuthenticator.gssAcceptor(service),
                new KerberosSessionToken(sessionSecret, Clock.systemUTC()));
    }
}
