# Capturing the mobile "change username" request

The web rename endpoint gets blocked fast. The Instagram **Android app** changes
a username through a mobile endpoint that is far harder to block. To wire that
into the claimer we need the **exact** request the app sends — guessing the body
or the signing breaks it. Capture it once and send it; then it gets wired in.

You need: an Android phone (or emulator) with the Instagram app, and a way to
see its HTTPS traffic. Instagram **pins its certificate**, so a plain proxy is
not enough — one of the setups below is required.

---

## Option A — Rooted phone / emulator + HTTP Toolkit (easiest if you have root)

1. Install **HTTP Toolkit** (PC) and its **Android app**, or use an emulator.
2. In HTTP Toolkit choose **Android device via ADB** and connect the phone
   (USB debugging on). It installs its CA certificate.
3. On a **rooted** device, move the HTTP Toolkit CA into the **system** store
   (Magisk module "Move Certificates", or `adb`), so the pinned app trusts it.
4. To beat certificate **pinning**, run **Frida** with a pinning-bypass script
   (e.g. `frida-multiple-unpinning`) against the Instagram process, or use the
   HTTP Toolkit "Android (Frida)" interception if offered.
5. Now traffic is visible. Do the trigger (below) and copy the request.

## Option B — objection / Frida (rooted or patched app)

1. `pip install objection frida-tools`.
2. Patch or attach: `objection -g com.instagram.android explore`, then
   `android sslpinning disable`.
3. Point the app's traffic at your proxy (HTTP Toolkit / Burp) and do the
   trigger.

## Option C — No root: patched APK

1. Get the Instagram APK, patch it with **apk-mitm** (adds a user cert and
   disables pinning), install the patched APK.
2. Run HTTP Toolkit / Burp as the proxy, install its CA as a user cert, do the
   trigger.
   (Some app versions still refuse; a slightly older APK version often works.)

---

## The trigger (do this while capturing)

On the throwaway account, in the Instagram app:

1. **Profile** → **Edit profile**
2. Tap **Username**
3. Change it to anything and tap the **check / Done** to SAVE
4. Stop the capture

## Find the request

Look in the capture for a POST to one of these (filter by "edit" or "username"):

- `i.instagram.com/api/v1/accounts/edit_profile/`   ← most likely
- `i.instagram.com/api/v1/accounts/set_username/`
- `i.instagram.com/api/v1/bloks/...username...`      ← if this, tell me (it's the
  hard Bloks path)

## What to send back

Copy and paste, exactly:

1. **Method + URL** (full, with query if any)
2. **All request headers** — especially:
   `authorization`, `x-ig-app-id`, `content-type`, `user-agent`, and any
   `x-mid`, `x-ig-www-claim`, `x-bloks-version-id`
3. **The full request body** — the whole `signed_body=...` string / all the
   form fields (this is the part that matters most)
4. The **response status** (200 / other)

> Security: the request contains a live token. Send it from a **throwaway**
> account, and log that account out of all sessions after we finish.

Once I have that one request, the mobile claimer is wired into `mighty`
(`-claim` / `-claim-hook`) and pushed to the repo — no more web blocks.
