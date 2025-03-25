﻿using System;
using System.Buffers;
using System.Distributed.DurableTasks;
using System.Runtime.CompilerServices;
using Orleans.Runtime;
using Orleans.Serialization.Buffers;
using Orleans.Serialization.Codecs;
using Orleans.Serialization.WireProtocol;

namespace Orleans.DurableTasks;

/// <summary>
/// Functionality for serializing and deserializing <see cref="DurableTaskObserverAddress"/> instances.
/// </summary>
[RegisterSerializer]
public sealed class DurableTaskClientAddressCodec : IFieldCodec<DurableTaskObserverAddress>
{
    private readonly Type _codecType = typeof(DurableTaskObserverAddress);

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteField<TBufferWriter>(
        ref Writer<TBufferWriter> writer,
        uint fieldIdDelta,
        Type expectedType,
        DurableTaskObserverAddress value)
        where TBufferWriter : IBufferWriter<byte>
    {
        ReferenceCodec.MarkValueField(writer.Session);
        writer.WriteFieldHeader(fieldIdDelta, expectedType, _codecType, WireType.LengthPrefixed);
        var bytes = value.AsSpan();
        if (bytes.IsEmpty) writer.WriteByte(1); // Equivalent to `writer.WriteVarUInt32(0);`
        else
        {
            writer.WriteVarUInt32((uint)(sizeof(int) + bytes.Length));
            writer.WriteInt32(value.GetHashCode());
            writer.Write(bytes);
        }
    }

    /// <summary>
    /// Writes an <see cref="DurableTaskObserverAddress"/> value to the provided writer without field framing.
    /// </summary>
    /// <param name="writer">The writer.</param>
    /// <param name="value">The value to write.</param>
    /// <typeparam name="TBufferWriter">The underlying buffer writer type.</typeparam>
    public static void WriteRaw<TBufferWriter>(
        ref Writer<TBufferWriter> writer,
        DurableTaskObserverAddress value)
        where TBufferWriter : IBufferWriter<byte>
    {
        var bytes = value.AsSpan();
        writer.WriteVarUInt32((uint)bytes.Length);
        if (!bytes.IsEmpty)
        {
            writer.WriteInt32(value.GetHashCode());
            writer.Write(bytes);
        }
    }

    /// <summary>
    /// Reads an <see cref="DurableTaskObserverAddress"/> value from a reader without any field framing.
    /// </summary>
    /// <typeparam name="TInput">The underlying reader input type.</typeparam>
    /// <param name="reader">The reader.</param>
    /// <returns>An <see cref="DurableTaskObserverAddress"/>.</returns>
    public static DurableTaskObserverAddress ReadRaw<TInput>(ref Reader<TInput> reader)
    {
        var length = reader.ReadVarUInt32();
        if (length == 0)
            return default;

        var hashCode = reader.ReadInt32();
        var payloadArray = reader.ReadBytes(length);
        return new(IdSpan.UnsafeCreate(payloadArray, hashCode));
    }

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public DurableTaskObserverAddress ReadValue<TInput>(ref Reader<TInput> reader, Field field)
    {
        field.EnsureWireType(WireType.LengthPrefixed);
        ReferenceCodec.MarkValueField(reader.Session);

        var length = reader.ReadVarUInt32();
        if (length == 0)
            return default;

        var hashCode = reader.ReadInt32();
        var payloadArray = reader.ReadBytes(length - sizeof(int));
        return new(IdSpan.UnsafeCreate(payloadArray, hashCode));
    }
}
