noVNC core v1.6.0, unmodified, licensed MPL-2.0 (see LICENSE.txt).

Source: https://github.com/novnc/noVNC/releases/tag/v1.6.0
Archive: https://codeload.github.com/novnc/noVNC/tar.gz/refs/tags/v1.6.0
SHA-256: 5066103959ef4e9b10f37e5a148627360dd8414e4cf8a7db92bdbd022e728aaa

Only core/, vendor/, AUTHORS and the license files are included. Pako's
included zlib license and the other upstream license files are retained.
The browser imports these modules directly. No package install, CDN or build
step is needed. Wefty authenticates the person over the Fabric; the Seat's
RFB backend uses the None security type, so Web Crypto is not needed.

Wefty's page subclasses the pinned core only to advertise stateless Raw
rectangles (plus DesktopSize and LastRect) and to replay its pixel format,
encodings and a full update request after take/release or another person's
override. The existing front door changes backend legs without replaying
client display state. Raw avoids retaining a compression stream across those
legs, at the cost of more bandwidth on the local Fabric. The renderer and
input handling otherwise remain upstream noVNC.
