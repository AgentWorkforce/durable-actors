---
name: durable-actors-quickstart
description: "Use when the user wants to build an app with Durable Actors or Terse actors, from scaffold to local dev to deploy"
---

# Durable Actors Quickstart

Stand up a minimal Terse Durable Actors project from the user's idea locally. Keep it minimal by design: local setup, no publish step just yet. 

## Workflow

1. Ensure all dependencies are installed (bun). Default to using the managed service terse cli unless explicitly asked to use the durable-actors cli or the open source CLI is already installed or the open source version is explicitly asked for.
2. Run terse init, giving a suitable name for the project if the user doesn't suggest one already and install the project dependencies.
3. Present a short plan and CONFIRM before building features beyond the template.
4. Stand up the actor dev server.
5. Build out the user's idea.
6. Generated a typed SDK to be used by backend/frontend.
7. Make sure to verify the solution including typechecking and testing out end to end at least once.
8. When done, offer to open the observability UI. Also if on the managed version, offer to deploy their project into the cloud. After a deploy, regenerate the client against the deployed actors and rerun the end-to-end test against the cloud.

## Rules

- For web apps with frontend and backend: Backend calls actor methods over RPC and issues WebSocket grants. The frontend never uses RPC; it connects directly to the actor over WebSocket.
- For the CLI, use --help to understand how to use the CLI.
- Read the type definitions in durable-actors
- Use docs.useterse.ai if unclear
- Use https://github.com/TerseAI/Terse-Examples if you want to see what the templates look like before installing them.
- Mark durable fields with @Persisted; anything else is lost on restart. @Persisted saves on assignment, so reassign arrays and objects (`this.list = [...this.list, x]`) instead of mutating them in place.
- Never edit generated/ by hand.
- Do not rerun init once it has succeeded.
- Do not put TERSE_API_KEY in client side code like a browser or a CLI installed on a user's computer.
- Never ask the user to paste an API key into the chat, and never print or read `.env`. Have the user write the key into the gitignored `.env` themselves.
- Every method on the actor class is an actor method and must be async, including Typescript private ones. Use #private for helpers.


