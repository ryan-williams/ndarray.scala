package org.lasersonlab.ndview

import java.time.Instant
import java.time.Instant.ofEpochSecond

import org.lasersonlab.files.http
import org.lasersonlab.gcp.googleapis.{ Paged, User }
import org.lasersonlab.gcp.googleapis.projects.Project
import org.lasersonlab.gcp.oauth.Auth.Valid
import org.lasersonlab.gcp.oauth.{ Auth, ClientId, Params, RedirectUrl, Scope, Scopes }
import org.lasersonlab.ndview.model.{ Login, Projects }
import org.lasersonlab.test.future.Assert
import utest._

object CircuitTest
  extends lasersonlab.Suite
     with http.Test
     with Assert.syntax {

  val login =
    Login(
      Auth(
        token = "token",
        granted = ofEpochSecond(123),
        expires = ofEpochSecond(456),
        scopes = Vector("scope1"),
        params = Params(
          ClientId("ClientId"),
          RedirectUrl("RedirectUrl"),
          Scopes(
            Scope("scope1")
          )
        ),
        state = Valid
      ),
      User(
        id = "id",
        name = "name",
        email = Some("email"),
        picture = "picture",
      ),
      Projects(
        Paged(
          Vector(
            Project(
              name = "name",
              id = "id",
              number = "number",
              buckets = None
            )
          )
        )
      )
    )

  val tests = Tests {
    'simple - {
      val circuit = Circuit(Model())
      circuit.dispatch(
        NewLogin(login)
      )
      val actual = circuit.zoom(x ⇒ x).value
      type A[T] = Assert[T, T]
      implicitly[A[String]]
      implicitly[A[Instant]]
      implicitly[A[User]]
      implicitly[A[Auth]]
      implicitly[A[Projects]]
      implicitly[A[Login]]
      ==(
        actual,
        Model()
      )
    }
  }
}
